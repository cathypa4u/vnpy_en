from typing import Any

from markdown_it import MarkdownIt
from markdown_it.token import Token

from vnag.object import Segment
from vnag.segmenter import BaseSegmenter, pack_section


class MarkdownSegmenter(BaseSegmenter):
    """
    Markdown 文本分段器，它利用标题（Headings）来创建结构化的文本段。
    """

    def __init__(self, chunk_size: int = 2000) -> None:
        """
        初始化 MarkdownSegmenter。

        参数:
            chunk_size: 每个文本块的最大长度，默认为 2000。
        """
        self.chunk_size: int = chunk_size
        self.md_parser: MarkdownIt = MarkdownIt()

    def parse(self, text: str, metadata: dict[str, Any]) -> list[Segment]:
        """
        将输入的 Markdown 文本分割成一系列结构化的 Segment。

        处理流程:
        1. 使用 markdown-it-py 解析文本，获取 Tokens。
        2. 调用 `group_by_headings` 方法，按标题将 Tokens 分割成逻辑章节。
        3. 调用 `pack_section` 进行三层分块，确保每个块不超过 `chunk_size`。
        4. 为每个最终的文本块创建 `Segment` 对象，并附加元数据。
        """
        tokens: list[Token] = self.md_parser.parse(text)
        sections: list[tuple[str, str]] = group_by_headings(text, tokens)

        segments: list[Segment] = []
        segment_index: int = 0
        section_order: int = 0

        for title, content in sections:
            #Unified use of common boxing logic
            chunks: list[str] = pack_section(content, self.chunk_size)

            total_chunks: int = len(chunks)
            for i, chunk in enumerate(chunks):
                if not chunk.strip():
                    continue

                #Create an independent copy of metadata for each text block and add segmentation information
                chunk_meta: dict[str, Any] = metadata.copy()
                chunk_meta["chunk_index"] = str(segment_index)
                chunk_meta["section_order"] = str(section_order)
                chunk_meta["section_part"] = f"{i + 1}/{total_chunks}"

                if title:
                    chunk_meta["section_title"] = title

                segments.append(Segment(text=chunk, metadata=chunk_meta))
                segment_index += 1

            section_order += 1

        return segments


def group_by_headings(text: str, tokens: list[Token]) -> list[tuple[str, str]]:
    """
    根据标题 Token 将 Markdown 文本分割成章节。

    参数:
        text: 原始 Markdown 文本。
        tokens: 由 markdown-it-py 解析生成的 Token 列表。

    返回:
        一个元组列表，每个元组包含 (章节标题, 章节内容)。
    """
    sections: list[tuple[str, str]] = []
    current_section_lines: list[str] = []
    current_title: str = "默认章节"  #Sets a default title for content at the beginning of the document, before the first title

    lines: list[str] = text.splitlines()

    #Find all header tokens and their line numbers
    heading_indices: dict[int, str] = {
        token.map[0]: token.content
        for token in tokens
        if token.type == "heading_open" and token.map
    }

    #If no heading is found, process the entire document as a chapter
    if not heading_indices:
        return [(current_title, text)]

    #Traverse row by row and group content according to title row
    for i, line in enumerate(lines):
        if i in heading_indices:
            #When encountering a new title, save the previously collected chapter content
            if current_section_lines:
                sections.append(
                    (current_title, "\n".join(current_section_lines).strip())
                )

            #Start a new chapter
            current_title = heading_indices[i]
            current_section_lines = [line]
        else:
            #Append the current line content to the current chapter
            current_section_lines.append(line)

    #After the traversal is completed, save the content of the last chapter
    if current_section_lines:
        sections.append((current_title, "\n".join(current_section_lines).strip()))

    return sections
