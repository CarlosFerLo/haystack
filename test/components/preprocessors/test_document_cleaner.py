# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
import logging

import pytest

from haystack import Document
from haystack.components.preprocessors import DocumentCleaner, DEFAULT_ID_GENERATOR, KEEP_ID


class TestDocumentCleaner:
    def test_init(self):
        cleaner = DocumentCleaner()
        assert cleaner.remove_empty_lines is True
        assert cleaner.remove_extra_whitespaces is True
        assert cleaner.remove_repeated_substrings is False
        assert cleaner.remove_substrings is None
        assert cleaner.remove_regex is None

    def test_to_dict_default(self):
        component = DocumentCleaner()
        data = component.to_dict()
        assert data == {
            "type": "haystack.components.preprocessors.document_cleaner.DocumentCleaner",
            "init_parameters": {
                "remove_empty_lines": True,
                "remove_extra_whitespaces": True,
                "remove_repeated_substrings": False,
                "remove_substrings": None,
                "remove_regex": None,
                "id_generator": "haystack.components.preprocessors.document_cleaner.DEFAULT_ID_GENERATOR",
            },
        }

    def test_to_dict_with_parameters(self):
        def my_id_generator(old_doc: Document, new_doc: Document) -> str:
            return "1234"

        component = DocumentCleaner(
            remove_empty_lines=False,
            remove_extra_whitespaces=False,
            remove_repeated_substrings=True,
            remove_substrings=["random"],
            remove_regex="(.*)",
            id_generator=my_id_generator,
        )
        data = component.to_dict()
        assert data == {
            "type": "haystack.components.preprocessors.document_cleaner.DocumentCleaner",
            "init_parameters": {
                "remove_empty_lines": False,
                "remove_extra_whitespaces": False,
                "remove_repeated_substrings": True,
                "remove_substrings": ["random"],
                "remove_regex": "(.*)",
                "id_generator": "preprocessors.test_document_cleaner.my_id_generator",
            },
        }

    def test_to_dict_with_lambda_id_generator(self):
        component = DocumentCleaner(
            remove_empty_lines=False,
            remove_extra_whitespaces=False,
            remove_repeated_substrings=True,
            remove_substrings=["random"],
            remove_regex="(.*)",
            id_generator=lambda old_doc, new_doc: old_doc.id,
        )
        data = component.to_dict()
        assert data == {
            "type": "haystack.components.preprocessors.document_cleaner.DocumentCleaner",
            "init_parameters": {
                "remove_empty_lines": False,
                "remove_extra_whitespaces": False,
                "remove_repeated_substrings": True,
                "remove_substrings": ["random"],
                "remove_regex": "(.*)",
                "id_generator": "preprocessors.test_document_cleaner.<lambda>",
            },
        }

    def test_from_dict(self):
        data = {
            "type": "haystack.components.preprocessors.document_cleaner.DocumentCleaner",
            "init_parameters": {
                "remove_empty_lines": False,
                "remove_extra_whitespaces": False,
                "remove_repeated_substrings": True,
                "remove_substrings": ["random"],
                "remove_regex": "(.*)",
                "id_generator": "haystack.components.preprocessors.document_cleaner.KEEP_ID",
            },
        }
        component = DocumentCleaner.from_dict(data)
        assert component.remove_empty_lines is False
        assert component.remove_extra_whitespaces is False
        assert component.remove_repeated_substrings is True
        assert component.remove_substrings == ["random"]
        assert component.remove_regex == "(.*)"
        assert component.id_generator is KEEP_ID

    def test_non_text_document(self, caplog):
        with caplog.at_level(logging.WARNING):
            cleaner = DocumentCleaner()
            cleaner.run(documents=[Document()])
            assert "DocumentCleaner only cleans text documents but document.content for document ID" in caplog.text

    def test_single_document(self):
        with pytest.raises(TypeError, match="DocumentCleaner expects a List of Documents as input."):
            cleaner = DocumentCleaner()
            cleaner.run(documents=Document())

    def test_empty_list(self):
        cleaner = DocumentCleaner()
        result = cleaner.run(documents=[])
        assert result == {"documents": []}

    def test_remove_empty_lines(self):
        cleaner = DocumentCleaner(remove_extra_whitespaces=False)
        result = cleaner.run(
            documents=[
                Document(
                    content="This is a text with some words. "
                    ""
                    "There is a second sentence. "
                    ""
                    "And there is a third sentence."
                )
            ]
        )
        assert len(result["documents"]) == 1
        assert (
            result["documents"][0].content
            == "This is a text with some words. There is a second sentence. And there is a third sentence."
        )

    def test_remove_whitespaces(self):
        cleaner = DocumentCleaner(remove_empty_lines=False)
        result = cleaner.run(
            documents=[
                Document(
                    content=" This is a text with some words. "
                    ""
                    "There is a second sentence.  "
                    ""
                    "And there  is a third sentence. "
                )
            ]
        )
        assert len(result["documents"]) == 1
        assert result["documents"][0].content == (
            "This is a text with some words. " "" "There is a second sentence. " "" "And there is a third sentence."
        )

    def test_remove_substrings(self):
        cleaner = DocumentCleaner(remove_substrings=["This", "A", "words", "🪲"])
        result = cleaner.run(documents=[Document(content="This is a text with some words.🪲")])
        assert len(result["documents"]) == 1
        assert result["documents"][0].content == " is a text with some ."

    def test_remove_regex(self):
        cleaner = DocumentCleaner(remove_regex=r"\s\s+")
        result = cleaner.run(documents=[Document(content="This is a  text with   some words.")])
        assert len(result["documents"]) == 1
        assert result["documents"][0].content == "This is a text with some words."

    def test_remove_repeated_substrings(self):
        cleaner = DocumentCleaner(
            remove_empty_lines=False, remove_extra_whitespaces=False, remove_repeated_substrings=True
        )

        text = """First PageThis is a header.
        Page  of
        2
        4
        Lorem ipsum dolor sit amet
        This is a footer number 1
        This is footer number 2This is a header.
        Page  of
        3
        4
        Sid ut perspiciatis unde
        This is a footer number 1
        This is footer number 2This is a header.
        Page  of
        4
        4
        Sed do eiusmod tempor.
        This is a footer number 1
        This is footer number 2"""

        expected_text = """First Page 2
        4
        Lorem ipsum dolor sit amet 3
        4
        Sid ut perspiciatis unde 4
        4
        Sed do eiusmod tempor."""
        result = cleaner.run(documents=[Document(content=text)])
        assert result["documents"][0].content == expected_text

    def test_copy_metadata(self):
        cleaner = DocumentCleaner()
        documents = [
            Document(content="Text. ", meta={"name": "doc 0"}),
            Document(content="Text. ", meta={"name": "doc 1"}),
        ]
        result = cleaner.run(documents=documents)
        assert len(result["documents"]) == 2
        assert result["documents"][0].id != result["documents"][1].id
        for doc, cleaned_doc in zip(documents, result["documents"]):
            assert doc.meta == cleaned_doc.meta
            assert cleaned_doc.content == "Text."

    def test_keep_id_generator(self):
        cleaner = DocumentCleaner(id_generator=KEEP_ID)

        documents = [
            Document(content="Text. ", meta={"name": "doc 0"}),
            Document(content="Text. ", meta={"name": "doc 1"}),
        ]
        result = cleaner.run(documents=documents)
        assert len(result["documents"]) == 2
        assert result["documents"][0].id != result["documents"][1].id
        for doc, cleaned_doc in zip(documents, result["documents"]):
            assert doc.id == cleaned_doc.id
            assert doc.meta == cleaned_doc.meta
            assert cleaned_doc.content == "Text."

    def test_custom_id_generator(self):
        def id_generator(old_doc: Document, new_doc: Document) -> str:
            return old_doc.id + "-new"

        cleaner = DocumentCleaner(id_generator=id_generator)

        documents = [
            Document(content="Text. ", meta={"name": "doc 0"}),
            Document(content="Text. ", meta={"name": "doc 1"}),
        ]
        result = cleaner.run(documents=documents)
        assert len(result["documents"]) == 2
        assert result["documents"][0].id != result["documents"][1].id
        for doc, cleaned_doc in zip(documents, result["documents"]):
            assert doc.id + "-new" == cleaned_doc.id
            assert doc.meta == cleaned_doc.meta
            assert cleaned_doc.content == "Text."
