from typing import Dict, Any
import io
from PIL import Image as PILImage
import PyPDF2
from pyspark.sql.types import StructType
from .pdf_partition_reader_base import PdfPartitionReaderBase


class PdfPartitionReaderPDFBox(PdfPartitionReaderBase):
    def __init__(self,
                 input_partition: Any,
                 read_data_schema: StructType,
                 options: Dict[str, str]):
        super().__init__(input_partition, read_data_schema, options)
        self.current_file = 0
        self.output_image_type = options.get("outputImageType", "PNG")
        self.page_num = int(input_partition.files[self.current_file].start)
        self.pdf_reader = None
        self.current_page = None

    def next(self) -> bool:
        """Move to the next page/file"""
        if self.current_file < len(self.input_partition.files):
            file = self.input_partition.files[self.current_file]

            if self.page_num == int(file.start):
                self.filename = str(file.filePath)
                # Open PDF file
                with open(self.filename, 'rb') as pdf_file:
                    self.pdf_reader = PyPDF2.PdfReader(pdf_file)

            self.page_num_cur = self.page_num

            if (self.page_num < file.length + file.start - 1 and
                    self.page_num < len(self.pdf_reader.pages)):
                self.page_num += 1
                self.current_page = self.pdf_reader.pages[self.page_num_cur]
                return True
            else:
                self.current_file += 1
                if self.current_file < len(self.input_partition.files):
                    self.page_num = int(self.input_partition.files[self.current_file].start)
                return True
        return False

    def get_searchable_text(self) -> str:
        """Extract text from the current page"""
        if self.current_page:
            return self.current_page.extract_text()
        return ""

    def render_image(self, resolution: int) -> bytes:
        """Render the current page as an image"""
        if not self.current_page:
            return bytes()

        # Convert PDF page to image
        # Note: This is a simplified version. You might need to use other libraries
        # like pdf2image for better quality rendering
        image_type = self.options.get("imageType", "RGB")

        # Create a BytesIO object to store the image
        image_buffer = io.BytesIO()

        # Convert PDF page to image (you might want to use pdf2image here)
        # This is a placeholder implementation
        img = PILImage.new(image_type, (612, 792), (255, 255, 255))
        img.save(image_buffer, format=self.output_image_type)

        return image_buffer.getvalue()

    def close(self):
        """Clean up resources"""
        super().close()
        if self.pdf_reader:
            self.pdf_reader = None