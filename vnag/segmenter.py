from abc import ABC, abstractmethod
from typing import Any

from .object import Segment


class BaseSegmenter(ABC):
    """
    文本分段器的抽象基类。

    该类定义了所有文本分段器需要遵循的接口。
    其核心职责是将长文本分割成一系列结构化的 `Segment` 对象。
    注意：本基类只负责分段逻辑，不涉及文件读取等 I/O 操作。
    """

    @abstractmethod
    def parse(self, text: str, metadata: dict[str, Any]) -> list[Segment]:
        """
        对传入的文本信息进行解析处理，返回处理好的 Segment 列表。

        参数:
            text: 待分段的原始文本。
            metadata: 与该文本关联的元数据字典，将被复制到每个生成的 Segment 中。

        返回:
            一个由 Segment 对象组成的列表，每个 Segment 代表一个文本片段。
        """
        pass

    @staticmethod
    def chunk_text(text: str, chunk_size: int, overlap: int = 0) -> list[str]:
        """
        将长文本按固定大小切片（支持重叠），并返回所有非空片段的列表。

        参数:
            text: 待切分的原始文本。
            chunk_size: 每个片段的最大长度。
            overlap: 相邻片段之间重叠的字符数，默认为 0 (不重叠)。

        返回:
            一个由非空文本片段字符串组成的列表。

        注意:
            - 为了性能，片段内容保持原文，仅在判断是否为空白时执行 strip() 操作。
            - 所有完全由空白字符组成的片段都将被丢弃。
            - 切分的步长 `stride` 计算方式为 `max(1, chunk_size - overlap)`。
        """
        if chunk_size <= 0:
            return []

        #Calculate the slicing step size, making sure the step size is at least 1
        stride: int = max(1, chunk_size - max(0, overlap))
        chunks: list[str] = []
        text_length: int = len(text)

        #Split according to the calculated step size and block size
        for i in range(0, text_length, stride):
            chunk: str = text[i:i + chunk_size]
            if chunk.strip():  #Add only if the fragment contains non-whitespace content
                chunks.append(chunk)

        return chunks


def pack_lines(lines: list[str], chunk_size: int) -> list[str]:
    """
    将代码行列表打包成不超过指定大小的文本块。

    该函数模拟将代码行一个个放入箱子（文本块）的过程，以尽可能填满每个箱子。

    参数:
        lines: 待打包的字符串代码行列表。
        chunk_size: 每个文本块的最大长度。

    返回:
        一个由打包好的文本块字符串组成的列表。

    注意:
        - 代码行之间使用 `\n` 连接，拼接长度会计入总长度。
        - 该函数不处理单行超长的情况，单个超长行会自成一个块。
    """
    chunks: list[str] = []
    buffer: list[str] = []
    buffer_len: int = 0

    for line in lines:
        separator_len: int = 1 if buffer else 0
        line_len: int = len(line)

        #Check whether the current line will be too long after adding it to the buffer
        if buffer_len + line_len + separator_len <= chunk_size:
            #Not too long: add to buffer
            buffer.append(line)
            buffer_len += line_len + separator_len
            continue

        #Already too long: first pack the current buffer into blocks
        if buffer:
            assembled_chunk: str = "\n".join(buffer).strip()
            if assembled_chunk:
                chunks.append(assembled_chunk)

        #Then use the current line as the start of a new buffer
        buffer = [line]
        buffer_len = line_len

    #Clear the remaining contents of the last buffer
    if buffer:
        assembled_chunk = "\n".join(buffer).strip()
        if assembled_chunk:
            chunks.append(assembled_chunk)

    return chunks


def pack_section(section_content: str, chunk_size: int) -> list[str]:
    """
    将一个大的代码章节（如整个函数或类）分割成多个大小合适的块。

    这是一个三层降级式的分块策略，以确保在任何情况下都能生成大小合规的块：
    1. 首先，尝试按“段落”（由空行分隔的代码块）进行聚合。这有助于保持代码的逻辑内聚性。
    2. 如果按段落聚合后仍有超长的块，则对这些块退而求其次，按“行”进行聚合。
    3. 如果单行本身就超长（极端情况），则强制按固定长度进行切分，作为最终的兜底保障。

    参数:
        section_content: 待分割的完整代码章节字符串。
        chunk_size: 每个块的最大长度限制。

    返回:
        一个由分割好的文本块字符串组成的列表。
    """
    #If the chapter itself is not too long, return it directly without splitting
    if len(section_content) <= chunk_size:
        return [section_content]

    #--- First level: Smart aggregation by "paragraph" (blank line) ---
    paragraphs: list[str] = [p.strip() for p in section_content.split("\n\n") if p.strip()]

    #Use a helper function to "box" multiple small paragraphs into a block that is not too long
    paragraph_chunks: list[str] = []
    buffer: list[str] = []
    buffer_len: int = 0
    for para in paragraphs:
        para_len: int = len(para)
        separator_len: int = 2 if buffer else 0  #Separate paragraphs with "\n\n"

        #If the current paragraph is added to the buffer, it will be too long
        if buffer_len + para_len + separator_len > chunk_size:
            #First pack the contents of the current buffer into a block
            if buffer:
                paragraph_chunks.append("\n\n".join(buffer))
            #Then use the current paragraph as the start of a new buffer
            buffer = [para]
            buffer_len = para_len
        else:
            #If it is not too long, add it to the buffer
            buffer.append(para)
            buffer_len += para_len + separator_len

    #Clear the last buffer
    if buffer:
        paragraph_chunks.append("\n\n".join(buffer))

    #--- Second level: Aggregate by "row" for blocks that are still too long ---
    line_chunks: list[str] = []
    for chunk in paragraph_chunks:
        #If this paragraph-based block is not too long, it is adopted directly
        if len(chunk) <= chunk_size:
            line_chunks.append(chunk)
            continue

        #If it is too long, split it into more fine-grained lines
        line_chunks.extend(pack_lines(chunk.splitlines(), chunk_size))

    #--- The third level: Forcing extremely long single lines, perform forced length cutting (cover the bottom) ---
    final_chunks: list[str] = []
    for chunk in line_chunks:
        #If this line-based block is not too long, it is adopted directly
        if len(chunk) <= chunk_size:
            final_chunks.append(chunk)
        else:
            #If the single line itself is too long, perform forced segmentation
            final_chunks.extend(
                [chunk[i:i + chunk_size] for i in range(0, len(chunk), chunk_size)]
            )

    return final_chunks
