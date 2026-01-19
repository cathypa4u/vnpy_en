from typing import cast

from ..engine import AgentEngine
from ..utility import WORKING_DIR
from ..agent import Profile, TaskAgent
from .. import __version__
from .widget import AgentWidget, ToolDialog, ModelDialog, ProfileDialog, GatewayDialog
from .setting import get_setting
from .qt import QtWidgets, QtGui, QtCore


class MainWindow(QtWidgets.QMainWindow):
    """Main window"""

    def __init__(self, engine: AgentEngine) -> None:
        """Constructor"""
        super().__init__()

        self.engine: AgentEngine = engine

        self.agent_widgets: dict[str, AgentWidget] = {}

        self.current_id: str = ""

        self.models: list[str] = self.engine.list_models()

        self.first_show: bool = True

        self.init_ui()
        self.load_data()

    def init_ui(self) -> None:
        """Initialize UI"""
        self.setWindowTitle(f"VeighNa Agent - {__version__} - [ {WORKING_DIR} ]")

        self.init_menu()
        self.init_widgets()
        self.init_tray()

        self.status_label: QtWidgets.QLabel = QtWidgets.QLabel()
        self.status_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.statusBar().addWidget(self.status_label, 1)

    def init_widgets(self) -> None:
        """Initialize central control"""
        #Conversation related on the left
        self.new_button: QtWidgets.QPushButton = QtWidgets.QPushButton("New session")
        self.new_button.setFixedHeight(50)
        self.new_button.clicked.connect(self.new_agent_widget)

        self.profile_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.profile_combo.setEditable(True)

        profile_line: QtWidgets.QLineEdit | None = self.profile_combo.lineEdit()
        if profile_line:
            profile_line.setReadOnly(True)
            profile_line.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        self.profile_model: QtGui.QStandardItemModel = QtGui.QStandardItemModel()
        self.profile_combo.setModel(self.profile_model)

        self.session_list: QtWidgets.QListWidget = QtWidgets.QListWidget()

        #Set up a custom style sheet
        stylesheet: str = """
            QListWidget::item {
                padding-top: 10px;
                padding-bottom: 10px;
                padding-left: 10px;
                border-radius: 12px;
            }
            QListWidget::item:hover {
                background-color: rgba(42, 92, 142, 0.3);
                color: white;
            }
            QListWidget::item:selected {
                background-color: #4a90e2;
                color: white;
            }
        """
        self.session_list.setStyleSheet(stylesheet)

        self.session_list.currentItemChanged.connect(self.on_current_item_changed)
        self.session_list.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.session_list.customContextMenuRequested.connect(self.on_menu_requested)
        self.session_list.installEventFilter(self)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("Agent", self.profile_combo)

        left_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        left_vbox.addWidget(self.session_list)
        left_vbox.addLayout(form)
        left_vbox.addWidget(self.new_button)

        left_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        left_widget.setLayout(left_vbox)
        left_widget.setFixedWidth(350)

        #Chat related on the right
        self.stacked_widget: QtWidgets.QStackedWidget = QtWidgets.QStackedWidget()

        #Main layout
        main_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        main_hbox.addWidget(left_widget)
        main_hbox.addWidget(self.stacked_widget)

        central_widget = QtWidgets.QWidget()
        central_widget.setLayout(main_hbox)
        self.setCentralWidget(central_widget)

    def init_menu(self) -> None:
        """Initialization menu"""
        menu_bar: QtWidgets.QMenuBar = self.menuBar()

        sys_menu: QtWidgets.QMenu = menu_bar.addMenu("System")
        sys_menu.addAction("Quit", self.quit_application)

        session_menu: QtWidgets.QMenu = menu_bar.addMenu("Session")
        session_menu.addAction("New session", self.new_agent_widget)
        session_menu.addAction("Rename session", self.rename_current_widget)
        session_menu.addAction("Delete session", self.delete_current_widget)

        function_menu: QtWidgets.QMenu = menu_bar.addMenu("Function")
        function_menu.addAction("AI service configuration", self.show_gateway_dialog)
        function_menu.addAction("Agent configuration", self.show_profile_dialog)
        function_menu.addAction("Tool browser", self.show_tool_dialog)
        function_menu.addAction("Model browser", self.show_model_dialog)

        help_menu: QtWidgets.QMenu = menu_bar.addMenu("Help")
        help_menu.addAction("Official website", self.open_website)
        help_menu.addAction("About", self.show_about)

    def init_tray(self) -> None:
        """Initialize system tray"""
        #Create tray icon
        self.tray_icon: QtWidgets.QSystemTrayIcon = QtWidgets.QSystemTrayIcon(self)
        self.tray_icon.setIcon(self.windowIcon())
        self.tray_icon.setToolTip(f"VeighNa Agent - {__version__}")

        #Create tray menu
        tray_menu: QtWidgets.QMenu = QtWidgets.QMenu(self)

        show_action: QtGui.QAction = tray_menu.addAction("Show")
        show_action.triggered.connect(self.show_normal)

        tray_menu.addSeparator()

        quit_action: QtGui.QAction = tray_menu.addAction("Quit")
        quit_action.triggered.connect(self.quit_application)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.activated.connect(
            lambda reason: self.show_normal() if reason == QtWidgets.QSystemTrayIcon.ActivationReason.Trigger else None
        )
        self.tray_icon.show()

    def update_profile_combo(self) -> None:
        """Update agent configuration drop-down box"""
        #Record the name of the currently selected item
        current_name: str = self.profile_combo.currentText()

        #Clear model
        self.profile_model.clear()

        #Load all agent configurations
        profiles: list[Profile] = self.engine.get_all_profiles()
        profile_names: list[str] = sorted([p.name for p in profiles])

        for name in profile_names:
            item = QtGui.QStandardItem(name)
            item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.profile_model.appendRow(item)

        #Set the currently selected item
        if current_name in profile_names:
            self.profile_combo.setCurrentText(current_name)
        else:
            self.profile_combo.setCurrentIndex(0)

    def show_gateway_dialog(self) -> None:
        """Display the Gateway connection configuration interface"""
        dialog: GatewayDialog = GatewayDialog(self)
        dialog.exec()

        #If the configuration is modified, prompt the user to restart
        if dialog.was_modified():
            QtWidgets.QMessageBox.information(
                self,
                "Configuration saved",
                "The AI ​​service configuration has been saved and needs to be restarted to take effect",
                QtWidgets.QMessageBox.StandardButton.Ok
            )

    def show_profile_dialog(self) -> None:
        """Display the agent management interface"""
        dialog: ProfileDialog = ProfileDialog(self.engine, self)
        dialog.setWindowState(QtCore.Qt.WindowState.WindowMaximized)
        dialog.exec()

        #Reload agent configuration
        self.update_profile_combo()

    def show_tool_dialog(self) -> None:
        """Show tools"""
        dialog: ToolDialog = ToolDialog(self.engine, self)
        dialog.setWindowState(QtCore.Qt.WindowState.WindowMaximized)
        dialog.exec()

    def show_model_dialog(self) -> None:
        """Display model"""
        dialog: ModelDialog = ModelDialog(self.engine, self)
        dialog.setWindowState(QtCore.Qt.WindowState.WindowMaximized)
        dialog.exec()

        for agent_widget in self.agent_widgets.values():
            agent_widget.load_favorite_models()

    def load_data(self) -> None:
        """Load agent configuration and all sessions"""
        self.update_profile_combo()

        self.load_agent_widgets()

    def load_agent_widgets(self) -> None:
        """Load all sessions"""
        agents: list[TaskAgent] = self.engine.get_all_agents()
        agents.sort(key=lambda a: a.id, reverse=True)

        for agent in agents:
            self.add_agent_widget(agent)

        if not self.agent_widgets:
            self.new_agent_widget()
        else:
            self.current_id = agents[0].id
            self.switch_agent_widget(self.current_id)

        self.update_agent_list()

    def update_agent_list(self) -> None:
        """Update session list UI"""
        #Block signals to avoid triggering recursion
        self.session_list.blockSignals(True)

        #Clear list
        self.session_list.clear()

        #Sort sessions (new sessions first)
        sorted_widgets = sorted(
            self.agent_widgets.values(),
            key=lambda w: w.agent.id,
            reverse=True
        )

        #Add session to list
        for widget in sorted_widgets:
            agent: TaskAgent = widget.agent
            item: QtWidgets.QListWidgetItem = QtWidgets.QListWidgetItem(agent.name)
            item.setData(QtCore.Qt.ItemDataRole.UserRole, agent.id)
            self.session_list.addItem(item)

            if agent.id == self.current_id:
                self.session_list.setCurrentItem(item)

        #Restore signal
        self.session_list.blockSignals(False)

    def new_agent_widget(self) -> None:
        """Create new session"""
        #Get the currently selected agent configuration name
        name: str = self.profile_combo.currentText()
        if not name:
            QtWidgets.QMessageBox.warning(self, "Mistake", "Please select an agent configuration first")
            return

        #Get agent configuration
        profile: Profile | None = self.engine.get_profile(name)
        if not profile:
            QtWidgets.QMessageBox.warning(self, "Mistake", f"Agent configuration not found: {name}")
            return

        #Create new agents and windows
        agent: TaskAgent = self.engine.create_agent(profile, save=True)
        self.add_agent_widget(agent)

        #Update list and switch to new window
        self.update_agent_list()
        self.switch_agent_widget(agent.id)

    def add_agent_widget(self, agent: TaskAgent) -> None:
        """Add session window"""
        widget: AgentWidget = AgentWidget(
            engine=self.engine,
            agent=agent,
            update_list=self.update_agent_list
        )
        self.stacked_widget.addWidget(widget)
        self.agent_widgets[agent.id] = widget

    def switch_agent_widget(self, session_id: str) -> None:
        """Switch sessions based on ID"""
        self.current_id = session_id

        widget: AgentWidget = self.agent_widgets[session_id]
        self.stacked_widget.setCurrentWidget(widget)
        self.update_agent_list()

    def rename_agent_widget(self, session_id: str) -> None:
        """Rename session"""
        widget: AgentWidget | None = self.agent_widgets.get(session_id)
        if not widget:
            return

        agent: TaskAgent = widget.agent
        text, ok = QtWidgets.QInputDialog.getText(
            self,
            "Rename session",
            "Please enter a new session name:",
            text=agent.name
        )

        if ok and text:
            widget.agent.rename(text)
            self.update_agent_list()

    def delete_agent_widget(self, session_id: str) -> None:
        """Delete session"""
        reply: QtWidgets.QMessageBox.StandardButton = QtWidgets.QMessageBox.question(
            self,
            "Delete session",
            "Are you sure you want to delete this conversation? This operation is irreversible",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.Yes,
        )

        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            #Remove the corresponding control
            widget: AgentWidget | None = self.agent_widgets.pop(session_id, None)
            if widget:
                #Delete from file system
                self.engine.delete_agent(session_id)

                self.stacked_widget.removeWidget(widget)
                widget.deleteLater()

            #If the current session is deleted, switch to another session
            if self.current_id == session_id:
                if self.agent_widgets:
                    self.current_id = next(iter(self.agent_widgets.keys()))
                    self.switch_agent_widget(self.current_id)
                else:
                    self.new_agent_widget()

            self.update_agent_list()

    def rename_current_widget(self) -> None:
        """Rename the currently selected session"""
        if not self.current_id:
            QtWidgets.QMessageBox.warning(self, "Warn", "No sessions selected")
            return

        self.rename_agent_widget(self.current_id)

    def delete_current_widget(self) -> None:
        """Delete the currently selected session"""
        if not self.current_id:
            QtWidgets.QMessageBox.warning(self, "Warn", "No sessions selected")
            return

        self.delete_agent_widget(self.current_id)

    def show_about(self) -> None:
        """Show about"""
        QtWidgets.QMessageBox.information(
            self,
            "About",
            (
                "VeighNa Agent\n"
                "\n"
                f"Version number: {__version__}\n"
                "\n"
                f"Working directory: {WORKING_DIR}"
            ),
            QtWidgets.QMessageBox.StandardButton.Ok
        )

    def open_website(self) -> None:
        """Open the official website"""
        QtGui.QDesktopServices.openUrl(QtCore.QUrl("https://www.github.com/vnpy/vnag"))

    def showEvent(self, event: QtGui.QShowEvent) -> None:
        """Override display events, check configuration when first displayed"""
        super().showEvent(event)

        if self.first_show:
            self.first_show = False

            #Delay the call to let the main interface complete rendering first
            QtCore.QTimer.singleShot(100, self.check_gateway_setting)

    def check_gateway_setting(self) -> None:
        """Check the Gateway configuration. If not configured, the configuration dialog box will pop up"""
        gateway_type = get_setting("gateway_type")
        if not gateway_type:
            self.show_gateway_dialog()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """Override close event, minimize to tray instead of exit"""
        event.ignore()
        self.hide()
        self.tray_icon.showMessage(
            "VeighNa Agent",
            "The program has been minimized to the system tray",
            QtWidgets.QSystemTrayIcon.MessageIcon.Information,
            2000
        )

    def on_tray_icon_activated(self, reason: QtWidgets.QSystemTrayIcon.ActivationReason) -> None:
        """Handle tray icon activation event"""
        if reason == QtWidgets.QSystemTrayIcon.ActivationReason.Trigger:
            self.show_normal()

    def show_normal(self) -> None:
        """Show main interface"""
        self.show()
        self.activateWindow()

    def quit_application(self) -> None:
        """Exit application"""
        self.tray_icon.hide()
        QtWidgets.QApplication.quit()

    def eventFilter(self, obj: QtCore.QObject, event: QtCore.QEvent) -> bool:
        """Event filter"""
        if obj is self.session_list and event.type() == QtCore.QEvent.Type.KeyPress:
            key_event: QtGui.QKeyEvent = cast(QtGui.QKeyEvent, event)
            if key_event.key() == QtCore.Qt.Key.Key_Delete:
                item: QtWidgets.QListWidgetItem = self.session_list.currentItem()
                if item:
                    self.delete_agent_widget(item.data(QtCore.Qt.ItemDataRole.UserRole))
                    return True

        return super().eventFilter(obj, event)

    def on_current_item_changed(
        self,
        current: QtWidgets.QListWidgetItem | None,
        previous: QtWidgets.QListWidgetItem | None
    ) -> None:
        """Handle the current list item change event (supports keyboard navigation)"""
        if current:
            session_id: str = current.data(QtCore.Qt.ItemDataRole.UserRole)
            self.switch_agent_widget(session_id)

    def on_menu_requested(self, pos: QtCore.QPoint) -> None:
        """Show the session context menu"""
        item: QtWidgets.QListWidgetItem | None = self.session_list.itemAt(pos)
        if not item:
            return

        session_id: str = item.data(QtCore.Qt.ItemDataRole.UserRole)

        menu: QtWidgets.QMenu = QtWidgets.QMenu(self)

        rename_action: QtGui.QAction = menu.addAction("Rename")
        rename_action.triggered.connect(lambda: self.rename_agent_widget(session_id))

        delete_action: QtGui.QAction = menu.addAction("Delete")
        delete_action.triggered.connect(lambda: self.delete_agent_widget(session_id))

        menu.exec(self.session_list.mapToGlobal(pos))
