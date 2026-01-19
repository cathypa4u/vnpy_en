from typing import Any
from collections.abc import Generator
from pathlib import Path

from clang import cindex
from clang.cindex import CursorKind

from vnag.object import Segment
from vnag.segmenter import BaseSegmenter, pack_section


class CppSegmenter(BaseSegmenter):
    """
    C++ 头/源文件分段器（基于 libclang AST），它利用抽象语法树（AST）来创建结构化的、
    符合语法结构的文本段。

    """

    def __init__(self, chunk_size: int = 2000) -> None:
        """Constructor"""
        self.chunk_size: int = chunk_size

    def parse(self, text: str, metadata: dict[str, Any]) -> list[Segment]:
        """
        将输入的 C++ 头/源文件文本分割成一系列结构化的 Segment。

        处理流程:
        1. 使用 libclang 解析 C++ 头/源文件，获取 AST。
        2. 深度遍历 AST，收集结构块（以行号为边界）。
        3. 若无块，则整体作为 module。
        4. 按起始行排序。
        5. 补充：文件头/尾的模块片段。
        6. 调用 `pack_section` 进行三层分块，确保每个块不超过 `chunk_size`。
        7. 为每个最终的文本块创建 `Segment` 对象，并附加元数据。
        """
        if not text.strip():
            return []

        #Prepare clang parameters (built-in default)
        clang_args: list[str] = ["-x", "c++-header", "-std=c++17"]

        #Parsing unit: directly use the real file path provided by the caller
        source_name: str = metadata["source"]

        #Automatically supplement common include paths (no need to configure include_dirs)
        #Note: To ensure "zero configuration availability", -I is only derived within the following bounded range:
        #1) The directory where the source file is located (such as demo/)
        #2) include/ in the directory where the source file is located (such as demo/include/, if it exists, add it)
        #3) The parent directory of the directory where the source file is located (such as sip/)
        #4) include/ of the parent directory (such as sip/include/)
        #This will cover the common layout of `knowledge/sip/{include,demo}`,
        #It will not cross the boundary to higher-level directories and avoid introducing unnecessary system/external paths
        if source_name:
            src_path: Path = Path(source_name)
            if src_path.exists():
                src_dir = src_path.parent
                candidates: list = [
                    src_dir,
                    src_dir / "include",
                    src_dir.parent,
                    src_dir.parent / "include",
                ]
                seen: set[str] = set()
                for cand in candidates:
                    try:
                        p: Path = cand.resolve()
                    except Exception:
                        continue
                    s: str = str(p)
                    if p.exists() and s not in seen:
                        clang_args.extend(["-I", s])
                        seen.add(s)

        #Unified boxing and metadata creation (aligned with PythonSegmenter style)
        segments: list[Segment] = []
        segment_index: int = 0
        section_order: int = 0

        sections: Generator[tuple[str, str, str, str, str], None, None] = ast_split(text, source_name, clang_args)

        for title, content, section_type, summary, signature in sections:
            if not content.strip():
                continue

            chunks: list[str] = pack_section(content, self.chunk_size)
            total_chunks: int = len(chunks)
            for i, chunk in enumerate(chunks):
                if not chunk.strip():
                    continue

                meta: dict[str, Any] = metadata.copy()
                meta["chunk_index"] = str(segment_index)
                meta["section_title"] = title
                meta["section_type"] = section_type
                if summary:
                    meta["summary"] = summary
                if signature:
                    meta["signature"] = signature
                meta["section_order"] = str(section_order)
                meta["section_part"] = f"{i + 1}/{total_chunks}"

                segments.append(Segment(text=chunk, metadata=meta))
                segment_index += 1

            section_order += 1

        return segments


def ast_split(text: str, source_name: str, clang_args: list[str]) -> Generator[tuple[str, str, str, str, str], None, None]:
    """
    使用 libclang 将 C++ 代码分割成结构化章节，依次产出：
    (section_title, content, section_type, summary, signature)
    section_title 优先使用限定名（Namespace::Class::Method），模块段为 "module"。
    """
    lines: list[str] = text.splitlines(keepends=True)

    try:
        translation_unit: Any = parse_translation_unit(source_name=source_name, text=text, clang_args=clang_args)
    except Exception:
        #Parsing failed: the entire article is treated as a module segment
        yield "module", text, "module", "", ""
        return

    blocks: list[tuple[int, int, str, str, str, str]] = []
    for cursor in translation_unit.cursor.get_children():
        collect_blocks(cursor, blocks, source_name)

    if not blocks:
        yield "module", text, "module", "", ""
        return

    #Header module section
    first_start: int = max(1, blocks[0][0])
    if first_start > 1:
        head_text: str = slice_source_by_lines(lines, 1, first_start - 1)
        if head_text.strip():
            yield "module", head_text, "module", "", ""

    #Structural segment
    blocks.sort(key=lambda x: x[0])
    for block_index, (start_line, end_line, title, kind, qualified_name, signature) in enumerate(blocks):
        real_end: int = max(end_line, blocks[block_index + 1][0] - 1) if block_index + 1 < len(blocks) else end_line
        body: str = slice_source_by_lines(lines, start_line, real_end)
        if not body.strip():
            continue
        summary: str = extract_summary(lines, start_line) if kind in ("class", "struct", "function") else ""
        if not signature and kind == "function":
            signature = extract_cpp_param_list(body)
        section_title: str = qualified_name if qualified_name and qualified_name != "<module>" else title
        yield section_title, body, kind, summary, signature

    #Tail module segment
    last_end: int = max(end for _, end, *_ in blocks)
    if last_end < len(lines):
        tail_text: str = slice_source_by_lines(lines, last_end + 1, len(lines))
        if tail_text.strip():
            yield "module", tail_text, "module", "", ""


def parse_translation_unit(source_name: str, text: str, clang_args: list[str]) -> Any:
    """使用 libclang 解析翻译单元（TranslationUnit）- 模块级工具。

    - source_name: 源文件名（用于相对 include 定位）
    - text: 源文件内容
    - args: 传递给 clang 的编译参数（-I/-std 等）
    """
    #1) Create clang index object (lightweight handle, reusable)
    #Note: There is no global cache here to keep the function pure and testable
    index = cindex.Index.create()

    #2) Bind the source code in memory to source_name through unsaved_files,
    #This allows clang to parse and use this text even if the file with the same name does not exist on the disk
    #Benefits: The caller does not need to write temporary files, improving ease of use and speed
    unsaved = [(source_name, text)]

    #3) Actually call libclang for parsing:
    #- path: use the source_name provided by the caller to facilitate relative include to take effect
    #- args: Compilation parameters (especially -I include directory and -std), missing will lead to incomplete symbol/type resolution
    #- unsaved: the memory source code bound above
    #- options: Keep 0 (default) here. If more stringent/faster is required, it can be evaluated by subsequent requirements
    tu = index.parse(
        path=source_name,
        args=clang_args,
        unsaved_files=unsaved,
        options=0,
    )
    return tu


def collect_blocks(cursor: Any, blocks: list[tuple[int, int, str, str, str, str]], source_name: str = "") -> None:
    """深度遍历 AST，收集结构块（以行号为边界）- 模块级工具。

    采集的块类型：namespace/class/struct/enum/function/ctor/dtor/typedef
    每个块记录：(start_line, end_line, section_title, section_type, qualified_name, signature)
    说明（差异点）：signature 在 C++ 中由 clang displayname/result_type 推导；Python 由 AST 重建。
    """

    #Traverse the direct child nodes of the current cursor one by one; implement depth-first traversal through recursion
    for child in cursor.get_children():
        kind = child.kind
        #Filter nodes without location information (cannot be mapped to specific source code line numbers)
        if not child.location or not child.extent:
            continue

        #Filter nodes that are not the current file (declaration from #include)
        if source_name and child.location.file:
            #Normalize paths for comparison (use / separator uniformly)
            child_file: str = str(child.location.file.name).replace("\\", "/")
            src_file: str = source_name.replace("\\", "/")
            if child_file != src_file:
                continue

        #Extract starting and ending lines (1-based). Some nodes may not have an end line, and the bottom line is start
        start = getattr(child.extent.start, "line", 1)
        end = getattr(child.extent.end, "line", start)

        #Namespace: Record the range and continue recursing its child nodes
        if kind in (CursorKind.NAMESPACE,):
            title = f"namespace {child.spelling or ''}".strip()
            qualified_name = get_qualified_name(child)
            blocks.append((start, end, title, "namespace", qualified_name, ""))
            #Continue to drill into the namespace and capture nested classes/functions and other structures
            collect_blocks(child, blocks, source_name)

        #Class/structure: Record its own range and recurse to collect its internal methods/nested classes
        elif kind in (CursorKind.CLASS_DECL, CursorKind.STRUCT_DECL):
            tag = "class" if kind == CursorKind.CLASS_DECL else "struct"
            title = f"{tag} {child.spelling}"
            qualified_name = get_qualified_name(child)
            blocks.append((start, end, title, tag, qualified_name, ""))
            collect_blocks(child, blocks, source_name)

        #Enumeration: only records ranges and qualified names
        elif kind in (CursorKind.ENUM_DECL,):
            title = f"enum {child.spelling}"
            qualified_name = get_qualified_name(child)
            blocks.append((start, end, title, "enum", qualified_name, ""))

        #Function/Method/Construction/Destruction: Record the range, qualified name, and try to generate a signature
        elif kind in (
            CursorKind.CXX_METHOD,
            CursorKind.FUNCTION_DECL,
            CursorKind.CONSTRUCTOR,
            CursorKind.DESTRUCTOR,
        ):
            name = child.spelling or "<anon>"
            title = f"func {name}"
            qualified_name = get_qualified_name(child)
            #Displayname is usually in the form: "bar(int x, int y = 1)"; result_type may be empty (such as ctor/dtor)
            display: str = getattr(child, "displayname", "") or ""
            result_type = getattr(getattr(child, "result_type", None), "spelling", "")
            signature: str = display
            #If there is a return type, it is assembled into the "(args) -> ReturnType" style to facilitate consistent comparison with Python
            if display and result_type and child.kind not in (CursorKind.CONSTRUCTOR, CursorKind.DESTRUCTOR):
                left_paren = display.find("(")
                right_paren = display.rfind(")")
                if left_paren != -1 and right_paren != -1 and right_paren > left_paren:
                    signature = display[left_paren:right_paren+1] + f" -> {result_type}"
            #Otherwise (construction/destruction/missing return type), the parameter part of displayname is retained as the signature, and the information is sufficient for retrieval and display
            blocks.append((start, end, title, "function", qualified_name, signature))

        elif kind in (CursorKind.TYPEDEF_DECL, CursorKind.TYPE_ALIAS_DECL):
            title = f"using {child.spelling}"
            qualified_name = get_qualified_name(child)
            blocks.append((start, end, title, "typedef", qualified_name, ""))

        else:
            #Other syntax entities (fields/aliases, etc.) continue recursively, capturing structured information at their sub-levels
            collect_blocks(child, blocks, source_name)


def get_qualified_name(cursor: Any) -> str:
    """Get the qualified name: concatenate Namespace::Class::Method (module-level tool) along the semantic parent node"""
    parts: list[str] = []
    cur = cursor
    while cur is not None and getattr(cur, "spelling", None):
        parts.append(cur.spelling)
        cur = getattr(cur, "semantic_parent", None)
    parts.reverse()
    return "::".join(parts) if parts else "<module>"


def slice_source_by_lines(lines: list[str], start_line_incl: int, end_line_incl: int) -> str:
    """Slice the source string list by 1-based line number (right end included) (module-level tool)"""
    s = max(1, start_line_incl) - 1
    e = max(s, end_line_incl)
    return "".join(lines[s:e])


def extract_summary(lines: list[str], start_line_incl: int) -> str:
    """
    从块起始行向上回溯，提取紧邻的注释首行作为摘要（模块级工具）。
    - 支持 // 连续单行注释与 /*...*/ 单行块注释；遇空行停止。
    """
    i: int = max(1, start_line_incl) - 2
    if i < 0:
        return ""
    collected: list[str] = []
    in_block: bool = False
    while i >= 0:
        line: str = lines[i].rstrip("\n")
        stripped: str = line.strip()
        if not stripped:
            break
        if stripped.startswith("//"):
            collected.append(stripped.lstrip('/').strip())
            i -= 1
            continue
        if stripped.endswith("*/") or in_block:
            in_block = True
            content = stripped
            if content.endswith("*/"):
                content = content[:-2].rstrip()
            if content.startswith("/*"):
                content = content[2:].lstrip()
            if content.startswith("*"):
                content = content[1:].lstrip()
            collected.append(content)
            if stripped.startswith("/*"):
                break
            i -= 1
            continue
        break
    if not collected:
        return ""
    collected.reverse()
    return collected[0].strip()


def extract_cpp_param_list(code: str) -> str:
    """
    轻量括号匹配，提取第一个函数参数列表，返回形如 "(int x, int y = 1)"（模块级工具）。
    """
    start = code.find('(')
    if start == -1:
        return ""
    depth = 0
    for i in range(start, len(code)):
        ch = code[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                return code[start:i+1].strip()
    return ""
