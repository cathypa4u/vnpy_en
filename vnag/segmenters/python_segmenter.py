import ast
from ast import stmt
from typing import Any
from collections.abc import Generator, Sequence

from vnag.object import Segment
from vnag.segmenter import BaseSegmenter, pack_section


class PythonSegmenter(BaseSegmenter):
    """
    Python 代码分段器，它利用抽象语法树（AST）来创建结构化的、
    符合语法结构的文本段。
    """

    def __init__(self, chunk_size: int = 2000) -> None:
        """
        初始化 PythonSegmenter。

        参数:
            chunk_size: 每个文本块的最大长度，默认为 2000。
        """
        self.chunk_size: int = chunk_size

    def parse(self, text: str, metadata: dict[str, Any]) -> list[Segment]:
        """
        将输入的 Python 源码分割成一系列结构化的 Segment。

        处理流程:
        1. 调用 `ast_split` 函数，按顶层函数和类定义将源码分割成逻辑章节。
        2. 调用 `pack_section` 进行三层分块，确保每个块不超过 `chunk_size`。
        3. 为每个最终的文本块创建 `Segment` 对象，并附加元数据。
        """
        if not text.strip():
            return []

        segments: list[Segment] = []
        segment_index: int = 0
        section_order: int = 0

        #Chapters that split code into function/class definitions based on AST
        sections: Generator[tuple[str, str, str, str, str], None, None] = ast_split(text)

        for title, content, section_type, summary, signature in sections:
            if not content.strip():
                continue

            #Call the three-layer chunking strategy function to divide the chapter content
            chunks: list[str] = pack_section(content, self.chunk_size)

            total_chunks: int = len(chunks)
            for i, chunk in enumerate(chunks):
                if not chunk.strip():
                    continue

                #Create an independent copy of metadata for each text block and add segmentation information
                chunk_meta: dict[str, Any] = metadata.copy()
                chunk_meta["chunk_index"] = str(segment_index)
                chunk_meta["section_title"] = title
                chunk_meta["section_type"] = section_type
                if summary:
                    chunk_meta["summary"] = summary
                if signature:
                    chunk_meta["signature"] = signature
                chunk_meta["section_order"] = str(section_order)
                chunk_meta["section_part"] = f"{i + 1}/{total_chunks}"

                segments.append(Segment(text=chunk, metadata=chunk_meta))
                segment_index += 1

            section_order += 1

        return segments


def ast_split(text: str) -> Generator[tuple[str, str, str, str, str], None, None]:
    """
    使用 AST 将 Python 代码递归地分割成基于函数和类的章节。

    该函数会依次产出 (yield) 在模块顶-层及类内部定义的每个函数、类，
    以及它们之间的代码块。对于类内部的方法，其标题会自动添加类名作为前缀。

    参数:
        text: 待分割的 Python 源码字符串。

    返回:
        一个生成器，每次产出一个元组 (章节标题, 章节内容, 章节类型, 摘要, 签名)。
    """
    try:
        tree: ast.Module = ast.parse(text)
    except SyntaxError:
        #If the code has syntax errors, AST parsing will fail
        #As a fallback strategy, treat the entire file as a single "module" block
        yield "module", text, "module", "", ""
        return

    lines: list[str] = text.splitlines(keepends=True)

    #---The main process begins ---
    #1. Output the code in the header of the file (before the first AST node)
    module_body = tree.body
    if module_body:
        first_node_start_line: int = module_body[0].lineno - 1
        if first_node_start_line > 0:
            module_header: str = "".join(lines[0:first_node_start_line]).strip()
            if module_header:
                yield "module", module_header, "module", "", ""

    #2. Recursively traverse the entire AST starting from the top level
    yield from traverse_body(module_body, lines)

    #3. Output the code at the end of the file (after the last AST node)
    if module_body:
        last_node_end_line: int = getattr(module_body[-1], "end_lineno", -1)
        if last_node_end_line != -1 and last_node_end_line < len(lines):
            remaining_code: str = "".join(lines[last_node_end_line:]).strip()
            if remaining_code:
                yield "module", remaining_code, "module", "", ""


def traverse_body(
    body_nodes: Sequence[stmt],
    lines: list[str],
    prefix: str = ""
) -> Generator[tuple[str, str, str, str, str], None, None]:
    """Recursively traverse the AST node body and produce code blocks"""
    #If the node body is empty, it is returned directly
    if not body_nodes:
        return

    #Initialize last_end_line to the position before the start of the first node
    #AST line numbers are 1-based and our indexes are 0-based
    last_end_line: int = body_nodes[0].lineno - 1

    #Traverse all nodes of the current level
    for node in body_nodes:
        start_line: int = node.lineno - 1
        end_line: int = getattr(node, "end_lineno", start_line)

        #Output the code between the previous node and the current node (such as comments or module-level code)
        if start_line > last_end_line:
            inter_code: str = "".join(lines[last_end_line:start_line]).strip()
            if inter_code:
                title: str = f"{prefix}module" if prefix else "module"
                yield title, inter_code, "module", "", ""

        #--- Process the current node ---
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            #If it is a function or asynchronous function, the entire function body is directly output
            node_code: str = "".join(lines[start_line:end_line]).strip()
            section_type: str = get_function_type(node, prefix)
            summary: str = ast.get_docstring(node) or ""
            signature: str = get_signature_string(node)
            yield f"{prefix}{node.name}", node_code, section_type, summary, signature

        elif isinstance(node, ast.ClassDef):
            #If it is a class definition, it needs to be processed recursively
            #Find where the "head" of the class ends (i.e. before the first method or subclass begins)
            header_end_line: int = end_line
            if node.body:
                #If the class body is not empty, the header ends before the first child node begins
                header_end_line = node.body[0].lineno - 1

            #Produces the header of the class (class ...:, docstring, class variables, etc.)
            header_code: str = "".join(lines[start_line:header_end_line]).strip()
            if header_code:
                summary = ast.get_docstring(node) or ""
                yield f"{prefix}{node.name}", header_code, "class", summary, ""

            #Calls itself recursively, processing methods and nested classes in the class body
            #The new prefix is ​​the current class name + "."
            new_prefix: str = f"{prefix}{node.name}."
            yield from traverse_body(node.body, lines, prefix=new_prefix)

            #Outputs everything after the closing curly brace of the class definition but before end_lineno
            #(This section is usually empty, but is reserved for rigor)
            if node.body:
                last_child_end_line = getattr(node.body[-1], "end_lineno", -1)
                if last_child_end_line != -1 and end_line > last_child_end_line:
                    footer_code = "".join(lines[last_child_end_line:end_line]).strip()
                    if footer_code:
                        #The footer section does not have a separate docstring, so the summary is empty
                        yield f"{prefix}{node.name}", footer_code, "class", "", ""
        else:
            #For other types of top-level statements (such as assignments, imports, etc.), they are classified as module code
            node_code = "".join(lines[start_line:end_line]).strip()
            if node_code:
                title = f"{prefix}module" if prefix else "module"
                yield title, node_code, "module", "", ""

        #Update the end position of the previous node
        last_end_line = end_line


def get_function_type(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    prefix: str
) -> str:
    """Determine function type based on context and decorators"""
    #If there is no prefix, it means it is an independent function at the top level of the module
    if not prefix:
        return "function"

    #There is a prefix indicating that it is a certain method inside the class
    #Check for specific decorators to further refine the type
    for decorator in node.decorator_list:
        #Decorators can be simple names (like @staticmethod) or calls (like @app.route('/'))
        #Here we only care about decorators of type ast.Name
        if isinstance(decorator, ast.Name):
            if decorator.id == 'classmethod':
                return "class_method"
            if decorator.id == 'staticmethod':
                return "static_method"

    #If no specific decorator is found, then a normal instance method
    return "method"


def get_signature_string(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Reconstruct a function's interface signature string from its AST node"""
    #Ast.unparse is available in Python 3.9+ and is used to convert AST nodes back to source strings
    if not hasattr(ast, 'unparse'):
        return ""  #Fails gracefully if the environment does not support ast.unparse

    parts = []
    args = node.args

    #1. Handle positional-only arguments (e.g., "a, b, /")
    if args.posonlyargs:
        for arg in args.posonlyargs:
            part = arg.arg
            if arg.annotation:
                part += f": {ast.unparse(arg.annotation)}"
            parts.append(part)
        parts.append("/")

    #2. Handle general parameters (e.g., "c, d=1")
    num_args_with_defaults = len(args.defaults)
    first_arg_with_default_idx = len(args.args) - num_args_with_defaults
    for i, arg in enumerate(args.args):
        part = arg.arg
        if arg.annotation:
            part += f": {ast.unparse(arg.annotation)}"
        if i >= first_arg_with_default_idx:
            default_val = args.defaults[i - first_arg_with_default_idx]
            part += f" = {ast.unparse(default_val)}"
        parts.append(part)

    #3. Processing *args
    if args.vararg:
        part = f"*{args.vararg.arg}"
        if args.vararg.annotation:
            part += f": {ast.unparse(args.vararg.annotation)}"
        parts.append(part)

    #4. Handle keyword-only arguments (e.g., "*, e, f=2")
    if args.kwonlyargs:
        if not args.vararg:
            parts.append("*")
        for i, arg in enumerate(args.kwonlyargs):
            part = arg.arg
            if arg.annotation:
                part += f": {ast.unparse(arg.annotation)}"
            kw_default_val = args.kw_defaults[i]
            if kw_default_val is not None:
                part += f" = {ast.unparse(kw_default_val)}"
            parts.append(part)

    #5. Handling **kwargs
    if args.kwarg:
        part = f"**{args.kwarg.arg}"
        if args.kwarg.annotation:
            part += f": {ast.unparse(args.kwarg.annotation)}"
        parts.append(part)

    #Combined into final signature
    signature = f"({', '.join(parts)})"
    if node.returns:
        signature += f" -> {ast.unparse(node.returns)}"

    return signature
