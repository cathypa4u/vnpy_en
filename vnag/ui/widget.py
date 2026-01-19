import json
import os
import uuid
from collections import defaultdict
from collections.abc import Callable
from typing import cast

from ..constant import Role
from ..engine import AgentEngine, default_profile
from ..object import ToolSchema
from ..agent import Profile, TaskAgent
from ..gateways import GATEWAY_CLASSES, get_gateway_class

from .qt import (
    QtCore,
    QtGui,
    QtWidgets,
    QtWebEngineCore,
    QtWebEngineWidgets
)
from .worker import StreamWorker
from .setting import (
    load_favorite_models,
    save_favorite_models,
    load_zoom_factor,
    save_zoom_factor,
    load_gateway_type,
    save_gateway_type,
    get_setting
)
from .factory import (
    load_gateway_setting,
    save_gateway_setting,
)


class HistoryWidget(QtWebEngineWidgets.QWebEngineView):
    """Session history control"""

    def __init__(self,  profile_name: str, parent: QtWidgets.QWidget | None = None) -> None:
        """Constructor"""
        super().__init__(parent)

        self.profile_name: str = profile_name

        #Set the page background color to transparent to avoid flickering when loading for the first time
        self.page().setBackgroundColor(QtGui.QColor("transparent"))

        #Streaming request related status
        self.full_content: str = ""
        self.full_thinking: str = ""
        self.msg_id: str = ""
        self.last_type: str = ""

        #Page load status and message queue
        self.page_loaded: bool = False
        self.message_queue: list[tuple[Role, str, str]] = []

        #Connection page loading complete signal
        self.page().loadFinished.connect(self._on_load_finished)

        #Connect permission request signal, handle clipboard permission
        self.page().permissionRequested.connect(self._on_permission_requested)

        #Load and apply saved zoom factor
        zoom_factor: float = load_zoom_factor()
        self.setZoomFactor(zoom_factor)

        #Connect the zoom change signal and automatically save the zoom factor
        self.page().zoomFactorChanged.connect(self._on_zoom_factor_changed)

        #Load local HTML file
        current_path: str = os.path.dirname(os.path.abspath(__file__))
        html_path: str = os.path.join(current_path, "resources", "chat.html")
        self.load(QtCore.QUrl.fromLocalFile(html_path))

    def _on_permission_requested(self, permission: QtWebEngineCore.QWebEnginePermission) -> None:
        """Handle permission requests and automatically grant clipboard permissions"""
        if permission.permissionType() == QtWebEngineCore.QWebEnginePermission.PermissionType.ClipboardReadWrite:
            permission.grant()

    def _on_zoom_factor_changed(self, zoom_factor: float) -> None:
        """Handle zoom factor changes and save automatically"""
        save_zoom_factor(zoom_factor)

    def _on_load_finished(self, success: bool) -> None:
        """Callback after page loading is complete"""
        if not success:
            return

        self._show_welcome_message()

        #Set the page loading completion flag and handle the message queue
        self.page_loaded = True

        for role, content, thinking in self.message_queue:
            self.append_message(role, content, thinking)

        self.message_queue.clear()

    def _show_welcome_message(self) -> None:
        """Show assistant welcome message"""
        js_content: str = json.dumps(f"Hello, my name is {self.profile_name}, how can I help you?")
        js_name: str = json.dumps(self.profile_name)
        self.page().runJavaScript(f"appendAssistantMessage({js_content}, {js_name})")

    def clear(self) -> None:
        """Clear session history"""
        if self.page_loaded:
            self.page().runJavaScript("document.getElementById('history').innerHTML = '';")
            self._show_welcome_message()
        else:
            self.message_queue.clear()

    def append_message(self, role: Role, content: str, thinking: str = "") -> None:
        """Add message in conversation history component"""
        #If the page is not loaded, add the message to the message queue
        if not self.page_loaded:
            self.message_queue.append((role, content, thinking))
            return

        #User messages, do not need to be rendered
        if role is Role.USER:
            escaped_content: str = (
                content.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\n", "<br>")
            )

            js_content: str = json.dumps(escaped_content)

            self.page().runJavaScript(f"appendUserMessage({js_content})")
        #AI messages need to be rendered
        elif role is Role.ASSISTANT:
            js_content = json.dumps(content)
            js_name: str = json.dumps(self.profile_name)
            js_thinking: str = json.dumps(thinking)
            self.page().runJavaScript(
                f"appendAssistantMessage({js_content}, {js_name}, {js_thinking})"
            )

    def start_stream(self) -> None:
        """Start a new streaming output"""
        #Clear the current streaming output content and message ID
        self.full_content = ""
        self.full_thinking = ""
        self.msg_id = f"msg-{uuid.uuid4().hex}"
        self.last_type = ""

        #Call the front-end function to start a new streaming output
        js_name: str = json.dumps(self.profile_name)
        self.page().runJavaScript(f"startAssistantMessage('{self.msg_id}', {js_name})")

    def update_stream(self, content_delta: str) -> None:
        """Update streaming output (content content)"""
        #The current type of record is content
        self.last_type = "content"

        #Cumulative content received
        self.full_content += content_delta

        #Convert content to JSON string
        js_content: str = json.dumps(self.full_content)

        #Call the front-end function to update the streaming output
        self.page().runJavaScript(f"updateAssistantMessage('{self.msg_id}', {js_content})")

    def update_thinking(self, thinking_delta: str) -> None:
        """Update streaming output (thinking content)"""
        #If other types of content (such as content) have been output before and there is already thinking content, then wrap the line
        if self.last_type and self.last_type != "thinking" and self.full_thinking:
            self.full_thinking += "\n\n"

        #The current type of record is thinking
        self.last_type = "thinking"

        #Accumulate received thinking content
        self.full_thinking += thinking_delta

        #Convert content to JSON string
        js_thinking: str = json.dumps(self.full_thinking)

        #Call the front-end function and update the thinking output
        self.page().runJavaScript(f"updateThinking('{self.msg_id}', {js_thinking})")

    def finish_stream(self) -> str:
        """End streaming output"""
        #Call the front-end function to end the streaming output
        self.page().runJavaScript(f"finishAssistantMessage('{self.msg_id}')")

        #Returns the complete streaming output content
        return self.full_content


class AgentWidget(QtWidgets.QWidget):
    """Session control"""

    def __init__(
        self,
        engine: AgentEngine,
        agent: TaskAgent,
        update_list: Callable[[], None],
        parent: QtWidgets.QWidget | None = None
    ) -> None:
        """Constructor"""
        super().__init__(parent)

        self.engine: AgentEngine = engine
        self.agent: TaskAgent = agent
        self.worker: StreamWorker | None = None
        self.update_list: Callable[[], None] = update_list

        self.init_ui()
        self.load_favorite_models()
        self.display_history()

    def init_ui(self) -> None:
        """Initialize UI"""
        desktop: QtCore.QRect = QtWidgets.QApplication.primaryScreen().availableGeometry()

        self.input_widget: QtWidgets.QTextEdit = QtWidgets.QTextEdit()
        self.input_widget.setMaximumHeight(desktop.height() // 4)
        self.input_widget.setPlaceholderText("Enter your message here and press enter or click the button to send")
        self.input_widget.setAcceptRichText(False)
        self.input_widget.installEventFilter(self)

        self.history_widget: HistoryWidget = HistoryWidget(profile_name=self.agent.profile.name)

        button_width: int = 80
        button_height: int = 50

        self.send_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Send")
        self.send_button.clicked.connect(self.send_message)
        self.send_button.setFixedWidth(button_width)
        self.send_button.setFixedHeight(button_height)

        self.stop_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Stop")
        self.stop_button.clicked.connect(self.stop_stream)
        self.stop_button.setFixedWidth(button_width)
        self.stop_button.setFixedHeight(button_height)
        self.stop_button.setVisible(False)

        self.resend_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Resend")
        self.resend_button.clicked.connect(self.resend_round)
        self.resend_button.setFixedWidth(button_width)
        self.resend_button.setFixedHeight(button_height)
        self.resend_button.setEnabled(False)

        self.delete_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Delete")
        self.delete_button.clicked.connect(self.delete_round)
        self.delete_button.setFixedWidth(button_width)
        self.delete_button.setFixedHeight(button_height)
        self.delete_button.setEnabled(False)

        self.model_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.model_combo.setFixedWidth(400)
        self.model_combo.setFixedHeight(50)
        self.model_combo.currentTextChanged.connect(self.on_model_changed)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addStretch()
        hbox.addWidget(self.model_combo)
        hbox.addWidget(self.delete_button)
        hbox.addWidget(self.resend_button)
        hbox.addWidget(self.stop_button)
        hbox.addWidget(self.send_button)

        vbox = QtWidgets.QVBoxLayout(self)
        vbox.addWidget(self.history_widget)
        vbox.addWidget(self.input_widget)
        vbox.addLayout(hbox)

    def display_history(self) -> None:
        """Show the chat history of the current session"""
        self.history_widget.clear()

        assistant_content: str = ""
        assistant_thinking: str = ""
        last_type: str = ""

        for message in self.agent.messages:
            #System message, not displayed
            if message.role is Role.SYSTEM:
                continue
            #User messages
            elif message.role is Role.USER:
                #There is content
                if message.content:
                    #If the assistant content is not empty, the assistant content (including previous tool call records) is displayed first
                    if assistant_content:
                        self.history_widget.append_message(
                            Role.ASSISTANT,
                            assistant_content,
                            assistant_thinking
                        )
                        assistant_content = ""
                        assistant_thinking = ""
                        last_type = ""

                    #Display user content
                    self.history_widget.append_message(Role.USER, message.content)
                #If there is no content (the tool call result is returned), skip
                else:
                    continue
            #Assistant message
            elif message.role is Role.ASSISTANT:
                #Accumulate thinking content
                if message.thinking:
                    #If other types of content (such as content) have been output before and there is already thinking content, then wrap the line
                    if last_type and last_type != "thinking" and assistant_thinking:
                        assistant_thinking += "\n\n"

                    assistant_thinking += message.thinking
                    last_type = "thinking"

                #If there is content, add it to the assistant content
                if message.content:
                    assistant_content += message.content
                    last_type = "content"

                #If there is a tool calling request, record the calling tool name
                if message.tool_calls:
                    for tool_call in message.tool_calls:
                        assistant_content += f"\n\n[Execution tool: {tool_call.name}]\n\n"
                    last_type = "tool"

        #Show message
        if assistant_content:
            self.history_widget.append_message(Role.ASSISTANT, assistant_content, assistant_thinking)

        self.update_buttons()

    def send_message(self) -> None:
        """Send message"""
        #Check if AI Gateway is configured
        gateway_type: str = get_setting("gateway_type")
        if not gateway_type:
            QtWidgets.QMessageBox.warning(
                self,
                "AI service is not configured",
                "Please first configure the AI ​​service in [Menu Bar-Function-AI Service Configuration]"
            )
            return

        model: str = self.model_combo.currentText()
        if not model:
            QtWidgets.QMessageBox.warning(
                self,
                "Model not selected",
                "Please first configure common models in [Menu Bar-Function-Model Browser]"
            )
            return

        text: str = self.input_widget.toPlainText().strip()
        if not text:
            return
        self.input_widget.clear()

        #Add user input to UI history
        self.history_widget.append_message(Role.USER, text)
        self.history_widget.start_stream()

        self.send_button.setVisible(False)
        self.stop_button.setVisible(True)
        self.resend_button.setEnabled(False)
        self.delete_button.setEnabled(False)

        worker: StreamWorker = StreamWorker(self.agent, text)
        worker.signals.delta.connect(self.on_stream_delta)
        worker.signals.thinking.connect(self.on_stream_thinking)
        worker.signals.finished.connect(self.on_stream_finished)
        worker.signals.error.connect(self.on_stream_error)
        worker.signals.title.connect(self.on_title_generated)

        self.worker = worker
        QtCore.QThreadPool.globalInstance().start(worker)

    def stop_stream(self) -> None:
        """Stop current streaming request"""
        if self.worker:
            self.worker.stop()

    def delete_round(self) -> None:
        """Delete last round of conversations"""
        self.agent.delete_round()
        self.display_history()

    def resend_round(self) -> None:
        """Resend last round of conversation"""
        prompt: str = self.agent.resend_round()

        if prompt:
            self.input_widget.setText(prompt)

        self.display_history()

    def update_buttons(self) -> None:
        """Update function button status"""
        if self.agent.messages and self.agent.messages[-1].role == Role.ASSISTANT:
            self.resend_button.setEnabled(True)
            self.delete_button.setEnabled(True)
        else:
            self.resend_button.setEnabled(False)
            self.delete_button.setEnabled(False)

    def eventFilter(self, obj: QtCore.QObject, event: QtCore.QEvent) -> bool:
        """Event filter"""
        if obj is self.input_widget and event.type() == QtCore.QEvent.Type.KeyPress:
            #Convert QEvent to QKeyEvent
            key_event: QtGui.QKeyEvent = cast(QtGui.QKeyEvent, event)
            if (
                key_event.key() in [QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter]
                and not key_event.modifiers() & QtCore.Qt.KeyboardModifier.ShiftModifier
            ):
                self.send_message()
                return True
        return super().eventFilter(obj, event)

    def on_stream_delta(self, content_delta: str) -> None:
        """Process the content data block returned by the data stream"""
        self.history_widget.update_stream(content_delta)

    def on_stream_thinking(self, thinking_delta: str) -> None:
        """Process the thinking data chunk returned by the data stream"""
        self.history_widget.update_thinking(thinking_delta)

    def on_stream_finished(self) -> None:
        """Handle data flow end event"""
        self.worker = None

        self.history_widget.finish_stream()
        self.update_buttons()

        self.send_button.setVisible(True)
        self.stop_button.setVisible(False)

    def on_stream_error(self, error_msg: str) -> None:
        """Handling data flow error events"""
        self.worker = None

        self.history_widget.finish_stream()
        self.update_buttons()

        self.send_button.setVisible(True)
        self.stop_button.setVisible(False)

        dialog = ErrorDialog("Streaming request failed:", error_msg, self)
        dialog.exec()

    def on_title_generated(self, title: str) -> None:
        """Processing title generation completed"""
        self.agent.rename(title)

        #Notify the main window to update the list
        self.update_list()

    def on_model_changed(self, model: str) -> None:
        """Handle model changes"""
        if model:
            self.agent.set_model(model)

    def load_favorite_models(self) -> None:
        """Load commonly used models"""
        current_text: str = self.model_combo.currentText()

        #Prevent signal from firing on_model_changed repeatedly
        self.model_combo.blockSignals(True)

        self.model_combo.clear()
        favorite_models: list[str] = load_favorite_models()

        #Only show models supported by the current gateway
        available_models: set[str] = set(self.engine.list_models())
        favorite_models = [m for m in favorite_models if m in available_models]

        self.model_combo.addItems(favorite_models)

        #Restore previous options
        if current_text in favorite_models:
            self.model_combo.setCurrentText(current_text)
        elif self.agent.model in favorite_models:
            self.model_combo.setCurrentText(self.agent.model)
        elif favorite_models:
            self.model_combo.setCurrentIndex(0)

        self.model_combo.blockSignals(False)

        #If model selection has changed after refresh, manually sync to Agent
        if self.model_combo.currentText() != self.agent.model:
            self.on_model_changed(self.model_combo.currentText())


class ErrorDialog(QtWidgets.QDialog):
    """Scrollable, copyable error message dialog"""

    def __init__(
        self,
        title: str,
        message: str,
        parent: QtWidgets.QWidget | None = None
    ) -> None:
        """Constructor"""
        super().__init__(parent)

        self.message: str = message

        self.setWindowTitle("Mistake")
        self.setMinimumSize(800, 600)

        layout = QtWidgets.QVBoxLayout(self)

        #Title tag
        label = QtWidgets.QLabel(title)
        layout.addWidget(label)

        #Scrollable, copyable text box
        text_edit = QtWidgets.QPlainTextEdit()
        text_edit.setPlainText(message)
        text_edit.setReadOnly(True)
        layout.addWidget(text_edit)

        #Button area
        button_layout = QtWidgets.QHBoxLayout()

        copy_button = QtWidgets.QPushButton("Copy")
        copy_button.clicked.connect(self.copy_message)
        button_layout.addWidget(copy_button)

        close_button = QtWidgets.QPushButton("Closure")
        close_button.clicked.connect(self.accept)
        button_layout.addWidget(close_button)

        layout.addLayout(button_layout)

    def copy_message(self) -> None:
        """Copy error message to clipboard"""
        QtWidgets.QApplication.clipboard().setText(self.message)


class ProfileDialog(QtWidgets.QDialog):
    """Agent management interface"""

    def __init__(self, engine: AgentEngine, parent: QtWidgets.QWidget | None = None):
        """"""
        super().__init__(parent)

        self.engine: AgentEngine = engine
        self.profiles: dict[str, Profile] = {}

        self.init_ui()
        self.load_profiles()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("Agent configuration")
        self.setMinimumSize(1000, 600)

        #Left list
        self.profile_list: QtWidgets.QListWidget = QtWidgets.QListWidget()
        self.profile_list.itemClicked.connect(self.on_profile_selected)

        #Right form
        self.name_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        self.prompt_text: QtWidgets.QTextEdit = QtWidgets.QTextEdit()

        #Temperature
        self.temperature_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        temperature_validator: QtGui.QDoubleValidator = QtGui.QDoubleValidator(0.0, 2.0, 1)
        temperature_validator.setNotation(QtGui.QDoubleValidator.Notation.StandardNotation)
        self.temperature_line.setValidator(temperature_validator)
        self.temperature_line.setPlaceholderText("Optional, between 0.0-2.0, 1 decimal place")

        #Maximum number of tokens
        self.tokens_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        max_tokens_validator: QtGui.QIntValidator = QtGui.QIntValidator(1, 10_000_000)
        self.tokens_line.setValidator(max_tokens_validator)
        self.tokens_line.setPlaceholderText("Optional, positive integer")

        self.iterations_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.iterations_spin.setRange(1, 200)
        self.iterations_spin.setSingleStep(1)
        self.iterations_spin.setValue(10)

        #Tool list
        self.tool_tree: QtWidgets.QTreeWidget = QtWidgets.QTreeWidget()
        self.tool_tree.setHeaderHidden(True)
        self.populate_tree()

        #Middle area form
        settings_form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        settings_form.addRow("Configuration name", self.name_line)
        settings_form.addRow("System prompt word", self.prompt_text)
        settings_form.addRow("Temperature", self.temperature_line)
        settings_form.addRow("Maximum number of tokens", self.tokens_line)
        settings_form.addRow("Maximum number of iterations", self.iterations_spin)

        middle_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        middle_widget.setLayout(settings_form)

        #Three column divider
        splitter = QtWidgets.QSplitter()
        splitter.addWidget(self.profile_list)
        splitter.addWidget(middle_widget)
        splitter.addWidget(self.tool_tree)
        splitter.setSizes([200, 500, 300])

        #Bottom button
        self.add_button: QtWidgets.QPushButton = QtWidgets.QPushButton("New")
        self.add_button.clicked.connect(self.new_profile)

        self.save_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Save")
        self.save_button.clicked.connect(self.save_profile)

        self.delete_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Delete")
        self.delete_button.clicked.connect(self.delete_profile)

        buttons_hbox = QtWidgets.QHBoxLayout()
        buttons_hbox.addStretch()
        buttons_hbox.addWidget(self.add_button)
        buttons_hbox.addWidget(self.save_button)
        buttons_hbox.addWidget(self.delete_button)

        #Main layout
        main_vbox = QtWidgets.QVBoxLayout()
        main_vbox.addWidget(splitter)
        main_vbox.addLayout(buttons_hbox)
        self.setLayout(main_vbox)

    def load_profiles(self) -> None:
        """Load configuration"""
        self.profile_list.clear()

        self.profiles = {p.name: p for p in self.engine.get_all_profiles()}

        for profile in self.profiles.values():
            item: QtWidgets.QListWidgetItem = QtWidgets.QListWidgetItem(profile.name, self.profile_list)
            item.setData(QtCore.Qt.ItemDataRole.UserRole, profile.name)

    def populate_tree(self) -> None:
        """Populate tool tree"""
        self.tool_tree.clear()

        #Add local tools
        local_tools: dict[str, ToolSchema] = self.engine.get_local_schemas()
        if local_tools:
            local_root = QtWidgets.QTreeWidgetItem(self.tool_tree, ["Local tools"])

            module_tools: dict[str, list[ToolSchema]] = defaultdict(list)
            for schema in local_tools.values():
                module, _ = schema.name.split("_", 1)
                module_tools[module].append(schema)

            for module, schemas in sorted(module_tools.items()):
                module_item = QtWidgets.QTreeWidgetItem(local_root, [module])
                module_item.setFlags(
                    module_item.flags()
                    | QtCore.Qt.ItemFlag.ItemIsUserCheckable
                    | QtCore.Qt.ItemFlag.ItemIsAutoTristate
                )
                module_item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)

                for schema in sorted(schemas, key=lambda s: s.name):
                    tool_item = QtWidgets.QTreeWidgetItem(module_item, [schema.name])
                    tool_item.setFlags(tool_item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                    tool_item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
                    tool_item.setData(0, QtCore.Qt.ItemDataRole.UserRole, schema.name)

        #Add MCP tool
        mcp_tools: dict[str, ToolSchema] = self.engine.get_mcp_schemas()
        if mcp_tools:
            mcp_root = QtWidgets.QTreeWidgetItem(self.tool_tree, ["MCP tools"])

            server_tools: dict[str, list[ToolSchema]] = defaultdict(list)
            for schema in mcp_tools.values():
                server, _ = schema.name.split("_", 1)
                server_tools[server].append(schema)

            for server, schemas in sorted(server_tools.items()):
                server_item = QtWidgets.QTreeWidgetItem(mcp_root, [server])
                server_item.setFlags(
                    server_item.flags()
                    | QtCore.Qt.ItemFlag.ItemIsUserCheckable
                    | QtCore.Qt.ItemFlag.ItemIsAutoTristate
                )
                server_item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)

                for schema in sorted(schemas, key=lambda s: s.name):
                    tool_item = QtWidgets.QTreeWidgetItem(server_item, [schema.name])
                    tool_item.setFlags(tool_item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                    tool_item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
                    tool_item.setData(0, QtCore.Qt.ItemDataRole.UserRole, schema.name)

        self.tool_tree.expandAll()

    def new_profile(self) -> None:
        """Create a new agent configuration"""
        self.profile_list.clearSelection()

        self.name_line.setReadOnly(False)
        self.name_line.clear()
        self.prompt_text.clear()

        self.temperature_line.clear()
        self.tokens_line.clear()
        self.iterations_spin.setValue(10)

        iterator = QtWidgets.QTreeWidgetItemIterator(self.tool_tree)
        while iterator.value():
            item = iterator.value()
            item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
            iterator += 1

        self.name_line.setFocus()

    def save_profile(self) -> None:
        """Save agent configuration"""
        name: str = self.name_line.text()
        if not name:
            QtWidgets.QMessageBox.warning(self, "Mistake", "Name cannot be empty!")
            return

        if name == default_profile.name:
            QtWidgets.QMessageBox.warning(self, "Mistake", "The default agent configuration cannot be modified!")
            return

        prompt: str = self.prompt_text.toPlainText()
        if not prompt:
            QtWidgets.QMessageBox.warning(self, "Mistake", "The system prompt word cannot be empty!")
            return

        temp_text: str = self.temperature_line.text()
        temperature: float | None = float(temp_text) if temp_text else None

        max_tokens_text: str = self.tokens_line.text()
        max_tokens: int | None = int(max_tokens_text) if max_tokens_text else None

        max_iterations: int = self.iterations_spin.value()

        selected_tools: list[str] = []
        iterator = QtWidgets.QTreeWidgetItemIterator(self.tool_tree)
        while iterator.value():
            item: QtWidgets.QTreeWidgetItem = iterator.value()
            if item.checkState(0) == QtCore.Qt.CheckState.Checked:
                tool_name: str = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
                if tool_name:  #Tool item, not classification
                    selected_tools.append(tool_name)
            iterator += 1

        #Update existing configuration
        if name in self.profiles:
            profile: Profile = self.profiles[name]

            profile.prompt = prompt
            profile.tools = selected_tools
            profile.temperature = temperature
            profile.max_tokens = max_tokens
            profile.max_iterations = max_iterations

            self.engine.update_profile(profile)
        #Create new configuration
        else:
            profile = Profile(
                name=name,
                prompt=prompt,
                tools=selected_tools,
                temperature=temperature,
                max_tokens=max_tokens,
                max_iterations=max_iterations,
            )
            self.engine.add_profile(profile)

        self.load_profiles()

        QtWidgets.QMessageBox.information(self, "Success", f"{name} agent configuration saved!", QtWidgets.QMessageBox.StandardButton.Ok)

    def delete_profile(self) -> None:
        """Delete agent configuration"""
        item: QtWidgets.QListWidgetItem | None = self.profile_list.currentItem()
        if not item:
            return

        profile_name: str = item.data(QtCore.Qt.ItemDataRole.UserRole)
        if profile_name == default_profile.name:
            QtWidgets.QMessageBox.warning(self, "Mistake", "The default agent configuration cannot be deleted!")
            return

        #Check agent dependencies
        agents: list[TaskAgent] = self.engine.get_all_agents()

        dependent_agents: list[str] = [a.name for a in agents if a.profile.name == profile_name]

        if dependent_agents:
            msg: str = "Unable to delete, the following agents are using this configuration: \n" + "\n".join(dependent_agents)
            QtWidgets.QMessageBox.warning(self, "Delete failed", msg)
            return

        reply: QtWidgets.QMessageBox.StandardButton = QtWidgets.QMessageBox.question(
            self,
            "Delete configuration",
            "Are you sure you want to delete this agent configuration?",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.No,
        )

        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            self.engine.delete_profile(profile_name)
            self.load_profiles()
            self.new_profile()

    def on_profile_selected(self, item: QtWidgets.QListWidgetItem) -> None:
        """Show selected agent configuration"""
        self.name_line.setReadOnly(True)

        profile_name: str = item.data(QtCore.Qt.ItemDataRole.UserRole)
        profile: Profile = self.profiles[profile_name]

        self.name_line.setText(profile.name)
        self.prompt_text.setPlainText(profile.prompt)

        if profile.temperature is not None:
            self.temperature_line.setText(str(profile.temperature))
        else:
            self.temperature_line.clear()

        if profile.max_tokens is not None:
            self.tokens_line.setText(str(profile.max_tokens))
        else:
            self.tokens_line.clear()

        self.iterations_spin.setValue(profile.max_iterations)

        #Only operate leaf nodes (tool items) and let AutoTristate automatically update the parent node
        iterator = QtWidgets.QTreeWidgetItemIterator(self.tool_tree)
        while iterator.value():
            tool_item: QtWidgets.QTreeWidgetItem = iterator.value()
            tool_name = tool_item.data(0, QtCore.Qt.ItemDataRole.UserRole)

            #Only process leaf nodes with UserRole data (tool item)
            if tool_name:
                if tool_name in profile.tools:
                    tool_item.setCheckState(0, QtCore.Qt.CheckState.Checked)
                else:
                    tool_item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)

            iterator += 1


class ToolDialog(QtWidgets.QDialog):
    """Dialog showing available tools"""

    def __init__(self, engine: AgentEngine, parent: QtWidgets.QWidget | None = None) -> None:
        """Constructor"""
        super().__init__(parent)

        self._engine: AgentEngine = engine

        self.init_ui()

    def init_ui(self) -> None:
        """Initialize UI"""
        self.setWindowTitle("Tool browser")
        self.setMinimumSize(800, 600)

        #Tree on left
        headers: list[str] = ["Classification", "Module", "Tool"]
        self.tree_widget: QtWidgets.QTreeWidget = QtWidgets.QTreeWidget()
        self.tree_widget.setColumnCount(len(headers))
        self.tree_widget.setHeaderLabels(headers)
        self.tree_widget.itemClicked.connect(self.on_item_clicked)
        self.tree_widget.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree_widget.customContextMenuRequested.connect(self.show_context_menu)

        #Details on the right
        self.detail_widget: QtWidgets.QTextEdit = QtWidgets.QTextEdit()
        self.detail_widget.setReadOnly(True)

        #Splitter
        splitter: QtWidgets.QSplitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        splitter.addWidget(self.tree_widget)
        splitter.addWidget(self.detail_widget)
        splitter.setSizes([250, 550])

        #Main layout
        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(splitter)
        self.setLayout(vbox)

        #Load data
        self.populate_tree()

    def populate_tree(self) -> None:
        """Populate tree"""
        self.tree_widget.clear()

        #Add local tools
        local_tools: dict[str, ToolSchema] = self._engine.get_local_schemas()
        if local_tools:
            local_root: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(
                self.tree_widget,
                ["Local tools", "", ""]
            )

            module_tools: dict[str, list[ToolSchema]] = defaultdict(list)
            for schema in local_tools.values():
                module, _ = schema.name.split("_", 1)
                module_tools[module].append(schema)

            for module, schemas in sorted(module_tools.items()):
                module_item: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(
                    local_root,
                    ["", module, ""]
                )
                for schema in sorted(schemas, key=lambda s: s.name):
                    _, name = schema.name.split("_", 1)
                    item: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(
                        module_item,
                        ["", "", name]
                    )
                    item.setData(0, QtCore.Qt.ItemDataRole.UserRole, schema)

            self.tree_widget.expandItem(local_root)

        #Add MCP tool
        mcp_tools: dict[str, ToolSchema] = self._engine.get_mcp_schemas()
        if mcp_tools:
            mcp_root: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(
                self.tree_widget,
                ["MCP tools", "", ""]
            )

            server_tools: dict[str, list[ToolSchema]] = defaultdict(list)
            for schema in mcp_tools.values():
                server, _ = schema.name.split("_", 1)
                server_tools[server].append(schema)

            for server, schemas in sorted(server_tools.items()):
                server_item: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(
                    mcp_root,
                    ["", server, ""]
                )
                for schema in sorted(schemas, key=lambda s: s.name):
                    _, name = schema.name.split("_", 1)
                    item = QtWidgets.QTreeWidgetItem(
                        server_item,
                        ["", "", name]
                    )
                    item.setData(0, QtCore.Qt.ItemDataRole.UserRole, schema)

            self.tree_widget.expandItem(mcp_root)

        for i in range(self.tree_widget.columnCount()):
            self.tree_widget.resizeColumnToContents(i)

    def on_item_clicked(self, item: QtWidgets.QTreeWidgetItem, column: int) -> None:
        """Handle item click event"""
        schema: ToolSchema | None = item.data(0, QtCore.Qt.ItemDataRole.UserRole)

        if schema:
            text: str = (
                f"[name]\n{schema.name}\n\n"
                f"[Description]\n{schema.description}\n\n"
                f"[Parameters]\n{json.dumps(schema.parameters, indent=4, ensure_ascii=False)}"
            )
            self.detail_widget.setText(text)

    def show_context_menu(self, pos: QtCore.QPoint) -> None:
        """Show right-click menu"""
        menu: QtWidgets.QMenu = QtWidgets.QMenu(self)

        expand_action: QtGui.QAction = menu.addAction("Expand all")
        expand_action.triggered.connect(self.tree_widget.expandAll)

        collapse_action: QtGui.QAction = menu.addAction("Collapse all")
        collapse_action.triggered.connect(self.tree_widget.collapseAll)

        menu.exec(self.tree_widget.viewport().mapToGlobal(pos))


class ModelDialog(QtWidgets.QDialog):
    """Dialog showing available models"""

    def __init__(self, engine: AgentEngine, parent: QtWidgets.QWidget | None = None) -> None:
        """Constructor"""
        super().__init__(parent)

        self._engine: AgentEngine = engine

        self.init_ui()

    def init_ui(self) -> None:
        """Initialize UI"""
        self.setWindowTitle("Model browser")
        self.setMinimumSize(800, 600)

        #All model trees on the left
        headers: list[str] = ["Manufacturer", "Model"]
        self.tree_widget: QtWidgets.QTreeWidget = QtWidgets.QTreeWidget()
        self.tree_widget.setColumnCount(len(headers))
        self.tree_widget.setHeaderLabels(headers)
        self.tree_widget.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree_widget.customContextMenuRequested.connect(self.show_context_menu)
        self.tree_widget.itemDoubleClicked.connect(self.add_model)

        #List of commonly used models on the right
        self.favorite_list: QtWidgets.QListWidget = QtWidgets.QListWidget()
        self.favorite_list.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.favorite_list.customContextMenuRequested.connect(self.show_favorite_context_menu)

        #Middle button
        add_button: QtWidgets.QPushButton = QtWidgets.QPushButton(">")
        add_button.clicked.connect(self.add_model)
        add_button.setFixedWidth(40)

        remove_button: QtWidgets.QPushButton = QtWidgets.QPushButton("<")
        remove_button.clicked.connect(self.remove_model)
        remove_button.setFixedWidth(40)

        up_button: QtWidgets.QPushButton = QtWidgets.QPushButton("↑")
        up_button.clicked.connect(self.move_model_up)
        up_button.setFixedWidth(40)

        down_button: QtWidgets.QPushButton = QtWidgets.QPushButton("↓")
        down_button.clicked.connect(self.move_model_down)
        down_button.setFixedWidth(40)

        button_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        button_vbox.addStretch()
        button_vbox.addWidget(add_button)
        button_vbox.addWidget(remove_button)
        button_vbox.addSpacing(20)
        button_vbox.addWidget(up_button)
        button_vbox.addWidget(down_button)
        button_vbox.addStretch()

        #Splitter
        splitter: QtWidgets.QSplitter = QtWidgets.QSplitter()
        splitter.addWidget(self.tree_widget)

        button_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        button_widget.setLayout(button_vbox)
        button_widget.setFixedWidth(60)
        splitter.addWidget(button_widget)

        splitter.addWidget(self.favorite_list)
        splitter.setSizes([350, 50, 400])
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)
        splitter.setStretchFactor(2, 4)

        #Bottom button
        self.save_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Save")
        self.save_button.clicked.connect(self.save_settings)

        buttons_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        buttons_hbox.addStretch()
        buttons_hbox.addWidget(self.save_button)

        #Main layout
        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout(self)
        vbox.addWidget(splitter)
        vbox.addLayout(buttons_hbox)

        self.populate_models()
        self.load_settings()

    def populate_models(self) -> None:
        """Populate all model trees"""
        models: list[str] = self._engine.list_models()

        separator: str | None = self.detect_separator(models)
        vendor_models: dict[str, list[str]] = defaultdict(list)

        if separator:
            for name in models:
                parts: list[str] = name.split(separator, 1)
                if len(parts) == 2:
                    vendor, model = parts
                    vendor_models[vendor].append(name)
                else:
                    vendor_models["Other"].append(name)
        else:
            for name in models:
                vendor_models["Other"].append(name)

        for vendor, model_list in sorted(vendor_models.items()):
            vendor_item: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(
                self.tree_widget,
                [vendor, ""]
            )
            for model_name in sorted(model_list):
                if separator:
                    _, model_display = model_name.split(separator, 1)
                else:
                    model_display = model_name

                item: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(vendor_item, ["", model_display])
                item.setData(0, QtCore.Qt.ItemDataRole.UserRole, model_name)

        self.tree_widget.expandAll()

        for i in range(self.tree_widget.columnCount()):
            self.tree_widget.resizeColumnToContents(i)

    def load_settings(self) -> None:
        """Load configuration"""
        self.favorite_list.clear()
        favorite_models: list[str] = load_favorite_models()
        self.favorite_list.addItems(favorite_models)

    def save_settings(self) -> None:
        """Save configuration"""
        models: list[str] = []
        for i in range(self.favorite_list.count()):
            item: QtWidgets.QListWidgetItem = self.favorite_list.item(i)
            models.append(item.text())

        save_favorite_models(models)
        QtWidgets.QMessageBox.information(self, "Success", "Commonly used model configurations have been saved!", QtWidgets.QMessageBox.StandardButton.Ok)

        self.close()

    def add_model(self) -> None:
        """Add model to favorite list"""
        item: QtWidgets.QTreeWidgetItem = self.tree_widget.currentItem()
        if not item:
            return

        model_name: str | None = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if not model_name:
            return

        current_models: list[str] = [
            self.favorite_list.item(i).text()
            for i in range(self.favorite_list.count())
        ]
        if model_name not in current_models:
            self.favorite_list.addItem(model_name)

    def remove_model(self) -> None:
        """Remove model from favorite list"""
        item: QtWidgets.QListWidgetItem = self.favorite_list.currentItem()
        if item:
            row: int = self.favorite_list.row(item)
            self.favorite_list.takeItem(row)

    def move_model_up(self) -> None:
        """Move up commonly used models"""
        current_row: int = self.favorite_list.currentRow()
        if current_row > 0:
            item: QtWidgets.QListWidgetItem = self.favorite_list.takeItem(current_row)
            self.favorite_list.insertItem(current_row - 1, item)
            self.favorite_list.setCurrentRow(current_row - 1)

    def move_model_down(self) -> None:
        """Move down commonly used models"""
        current_row: int = self.favorite_list.currentRow()
        if 0 <= current_row < self.favorite_list.count() - 1:
            item: QtWidgets.QListWidgetItem = self.favorite_list.takeItem(current_row)
            self.favorite_list.insertItem(current_row + 1, item)
            self.favorite_list.setCurrentRow(current_row + 1)

    def show_context_menu(self, pos: QtCore.QPoint) -> None:
        """Show right-click menu"""
        menu: QtWidgets.QMenu = QtWidgets.QMenu(self)

        expand_action: QtGui.QAction = menu.addAction("Expand all")
        expand_action.triggered.connect(self.tree_widget.expandAll)

        collapse_action: QtGui.QAction = menu.addAction("Collapse all")
        collapse_action.triggered.connect(self.tree_widget.collapseAll)

        menu.exec(self.tree_widget.viewport().mapToGlobal(pos))

    def show_favorite_context_menu(self, pos: QtCore.QPoint) -> None:
        """Display the common list right-click menu"""
        item: QtWidgets.QListWidgetItem | None = self.favorite_list.itemAt(pos)
        if not item:
            return

        menu: QtWidgets.QMenu = QtWidgets.QMenu(self)

        up_action: QtGui.QAction = menu.addAction("Move up")
        up_action.triggered.connect(self.move_model_up)

        down_action: QtGui.QAction = menu.addAction("Move down")
        down_action.triggered.connect(self.move_model_down)

        menu.addSeparator()

        remove_action: QtGui.QAction = menu.addAction("Remove")
        remove_action.triggered.connect(self.remove_model)

        menu.exec(self.favorite_list.viewport().mapToGlobal(pos))

    def detect_separator(self, models: list[str]) -> str | None:
        """Detect separators in model names"""
        if not models:
            return None

        candidates: list[str] = ["/", ":", "\\"]
        counts: dict[str, int] = defaultdict(int)

        for name in models:
            for sep in candidates:
                if sep in name:
                    counts[sep] += 1

        if not counts:
            return None

        return max(counts, key=lambda x: counts[x])


class GatewayDialog(QtWidgets.QDialog):
    """AI service configuration dialog box"""

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        """Constructor"""
        super().__init__(parent)

        self.setting_modified: bool = False

        #Nested dictionary: {gateway_type: {key: QLineEdit | QComboBox}}
        self.setting_widgets: dict[str, dict[str, QtWidgets.QWidget]] = {}

        self.page_indices: dict[str, int] = {}      #Gateway type to page index mapping

        self.init_ui()
        self.init_gateway_pages()
        self.load_current_setting()

    def init_ui(self) -> None:
        """Initialize UI"""
        self.setWindowTitle("AI service configuration")
        self.setMinimumSize(600, 300)

        #Gateway type selection
        self.type_label: QtWidgets.QLabel = QtWidgets.QLabel("AI service")

        self.type_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.type_combo.setFixedWidth(300)
        self.type_combo.addItems(sorted(GATEWAY_CLASSES.keys()))
        self.type_combo.currentTextChanged.connect(self.on_type_changed)

        type_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        type_hbox.addWidget(self.type_label)
        type_hbox.addWidget(self.type_combo)
        type_hbox.addStretch()

        #Configuring fields container - Preloading all pages using QStackedWidget
        self.setting_label: QtWidgets.QLabel = QtWidgets.QLabel("Configuration parameters")
        self.stack_widget: QtWidgets.QStackedWidget = QtWidgets.QStackedWidget()

        #Bottom button
        self.save_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Save")
        self.save_button.clicked.connect(self.save_setting)

        self.cancel_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)

        button_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        button_hbox.addStretch()
        button_hbox.addWidget(self.save_button)
        button_hbox.addWidget(self.cancel_button)

        #Main layout
        main_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        main_vbox.addLayout(type_hbox)
        main_vbox.addWidget(QtWidgets.QLabel("   "))
        main_vbox.addWidget(self.setting_label)
        main_vbox.addWidget(self.stack_widget)
        main_vbox.addLayout(button_hbox)
        self.setLayout(main_vbox)

    def init_gateway_pages(self) -> None:
        """Pre-create configuration pages for all Gateways"""
        for gateway_type in sorted(GATEWAY_CLASSES.keys()):
            gateway_cls = get_gateway_class(gateway_type)
            if not gateway_cls:
                continue

            #Create the page for the Gateway
            page_widget: QtWidgets.QWidget = QtWidgets.QWidget()
            page_layout: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
            page_widget.setLayout(page_layout)

            #Get default and saved configurations
            default_setting: dict = gateway_cls.default_setting
            saved_setting: dict = load_gateway_setting(gateway_type)

            #Create configuration fields
            widgets: dict[str, QtWidgets.QWidget] = {}
            for key, default_value in default_setting.items():
                label: str = self.get_field_label(key)

                #List types use QComboBox
                if isinstance(default_value, list):
                    combo_box: QtWidgets.QComboBox = QtWidgets.QComboBox()
                    combo_box.addItems(default_value)

                    #Set current options using saved values
                    saved_value: str = saved_setting.get(key, "")
                    if saved_value and saved_value in default_value:
                        combo_box.setCurrentText(saved_value)

                    page_layout.addRow(label, combo_box)
                    widgets[key] = combo_box
                #Other types use QLineEdit
                else:
                    line_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

                    #Use saved value, otherwise use default value
                    value: str = saved_setting.get(key, default_value)
                    line_edit.setText(str(value) if value else "")

                    page_layout.addRow(label, line_edit)
                    widgets[key] = line_edit

            #Save control references and page indexes
            self.setting_widgets[gateway_type] = widgets
            index: int = self.stack_widget.addWidget(page_widget)
            self.page_indices[gateway_type] = index

    def load_current_setting(self) -> None:
        """Load current configuration"""
        gateway_type: str = load_gateway_type()

        if gateway_type and gateway_type in GATEWAY_CLASSES:
            self.type_combo.setCurrentText(gateway_type)
        else:
            #The first one is selected by default
            self.type_combo.setCurrentIndex(0)

        self.on_type_changed(self.type_combo.currentText())

    def on_type_changed(self, gateway_type: str) -> None:
        """Switch display page when Gateway type changes"""
        if gateway_type in self.page_indices:
            self.stack_widget.setCurrentIndex(self.page_indices[gateway_type])

    def get_field_label(self, key: str) -> str:
        """Get field display label"""
        labels: dict[str, str] = {
            "base_url": "API address",
            "api_key": "API key",
            "reasoning_effort": "Reasoning strength",
        }
        return labels.get(key, key)

    def save_setting(self) -> None:
        """Save configuration"""
        gateway_type: str = self.type_combo.currentText()

        #Get the control of the current Gateway
        widgets: dict[str, QtWidgets.QWidget] | None = self.setting_widgets.get(
            gateway_type
        )
        if not widgets:
            return

        #Collect configuration values
        setting: dict[str, str] = {}
        for key, widget in widgets.items():
            if isinstance(widget, QtWidgets.QComboBox):
                setting[key] = widget.currentText()
            elif isinstance(widget, QtWidgets.QLineEdit):
                setting[key] = widget.text().strip()

        #Validate required fields
        gateway_cls = get_gateway_class(gateway_type)
        if gateway_cls:
            default_setting: dict = gateway_cls.default_setting
            for key in default_setting:
                #Api_key is required
                if key == "api_key" and not setting.get(key):
                    QtWidgets.QMessageBox.warning(
                        self,
                        "Configuration error",
                        "API key cannot be empty"
                    )
                    return

        #Save configuration
        save_gateway_type(gateway_type)
        save_gateway_setting(gateway_type, setting)

        self.setting_modified = True
        self.accept()

    def was_modified(self) -> bool:
        """Returns whether the configuration has been modified"""
        return self.setting_modified
