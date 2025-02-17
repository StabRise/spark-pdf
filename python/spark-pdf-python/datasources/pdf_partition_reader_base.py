from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import os
from PIL import Image as PILImage
import pytesseract
from pyspark.sql.types import StructType


class PdfPartitionReaderBase(ABC):
    def __init__(self,
                 input_partition: Any,
                 read_data_schema: StructType,
                 options: Dict[str, str]):
        self.input_partition = input_partition
        self.read_data_schema = read_data_schema
        self.options = options
        self.filename = ""
        self.tesseract = pytesseract.pytesseract
        self.page_num_cur = 0

        # Configure Tesseract
        ocr_config = options.get("ocrconfig", "psm=3")
        self.tesseract.config = ocr_config

    @abstractmethod
    def get_searchable_text(self) -> str:
        """Get searchable text from the current page"""
        pass

    @abstractmethod
    def render_image(self, resolution: int) -> bytes:
        """Render the current page as an image"""
        pass

    def get(self) -> Dict[str, Any]:
        """Get the current row data"""
        resolution = int(self.options.get("resolution", "300"))

        # Get text if needed
        text = ""
        if "text" in self.read_data_schema.fieldNames():
            text = self.get_searchable_text()

        # Get image if needed
        image_data = bytes()
        if "image" in self.read_data_schema.fieldNames() or "document" in self.read_data_schema.fieldNames():
            image_data = self.render_image(resolution)

        # Create image row
        image_row = {
            "path": self.filename,
            "resolution": resolution,
            "data": image_data,
            "imageType": "file",
            "exception": "",
            "height": 0,
            "width": 0
        }

        # Run OCR if needed
        ocr_document = {
            "path": self.filename,
            "text": "",
            "outputType": "",
            "bBoxes": [],
            "exception": ""
        }
        if "document" in self.read_data_schema.fieldNames():
            # Convert image_data to PIL Image
            img = PILImage.open(io.BytesIO(image_data))
            ocr_result = self.tesseract.image_to_data(img, output_type=pytesseract.Output.DICT)

            # Process OCR results
            boxes = []
            for i in range(len(ocr_result['text'])):
                if int(ocr_result['conf'][i]) > 0:  # Filter out low confidence results
                    boxes.append({
                        "text": ocr_result['text'][i],
                        "score": float(ocr_result['conf'][i]) / 100,
                        "x": ocr_result['left'][i],
                        "y": ocr_result['top'][i],
                        "width": ocr_result['width'][i],
                        "height": ocr_result['height'][i]
                    })

            ocr_document.update({
                "text": " ".join(box['text'] for box in boxes),
                "outputType": "word",
                "bBoxes": boxes
            })

        return {
            "path": self.filename,
            "filename": os.path.basename(self.filename),
            "page_number": self.page_num_cur,
            "partition_number": self.input_partition.index,
            "text": text,
            "image": image_row,
            "document": ocr_document
        }

    def close(self):
        """Clean up resources"""
        pass