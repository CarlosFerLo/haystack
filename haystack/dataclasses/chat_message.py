# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0

from dataclasses import asdict, dataclass, field, fields
from enum import Enum
from typing import Any, Dict, Optional


class ChatRole(str, Enum):
    """Enumeration representing the roles within a chat."""

    ASSISTANT = "assistant"
    USER = "user"
    SYSTEM = "system"
    FUNCTION = "function"


@dataclass
class ChatMessage:
    """
    Represents a message in a LLM chat conversation.

    :param content: The text content of the message.
    :param role: The role of the entity sending the message.
    :param name: The name of the function being called (only applicable for role FUNCTION).
    :param meta: Additional metadata associated with the message.
    """

    content: str
    role: ChatRole
    name: Optional[str]
    meta: Dict[str, Any] = field(default_factory=dict, hash=False)

    def to_openai_format(self) -> Dict[str, Any]:
        """
        Convert the message to the format expected by OpenAI's Chat API.

        See the [API reference](https://platform.openai.com/docs/api-reference/chat/create) for details.

        :returns: A dictionary with the following key:
            - `role`
            - `content`
            - `name` (optional)
        """
        msg = {"role": self.role.value, "content": self.content}
        if self.name:
            msg["name"] = self.name

        return msg

    def is_from(self, role: ChatRole) -> bool:
        """
        Check if the message is from a specific role.

        :param role: The role to check against.
        :returns: True if the message is from the specified role, False otherwise.
        """
        return self.role == role

    @classmethod
    def from_assistant(cls, content: str, meta: Optional[Dict[str, Any]] = None) -> "ChatMessage":
        """
        Create a message from the assistant.

        :param content: The text content of the message.
        :param meta: Additional metadata associated with the message.
        :returns: A new ChatMessage instance.
        """
        return cls(content, ChatRole.ASSISTANT, None, meta or {})

    @classmethod
    def from_user(cls, content: str) -> "ChatMessage":
        """
        Create a message from the user.

        :param content: The text content of the message.
        :returns: A new ChatMessage instance.
        """
        return cls(content, ChatRole.USER, None)

    @classmethod
    def from_system(cls, content: str) -> "ChatMessage":
        """
        Create a message from the system.

        :param content: The text content of the message.
        :returns: A new ChatMessage instance.
        """
        return cls(content, ChatRole.SYSTEM, None)

    @classmethod
    def from_function(cls, content: str, name: str) -> "ChatMessage":
        """
        Create a message from a function call.

        :param content: The text content of the message.
        :param name: The name of the function being called.
        :returns: A new ChatMessage instance.
        """
        return cls(content, ChatRole.FUNCTION, name)

    def to_dict(self, flatten: bool = True) -> Dict[str, Any]:
        """
        Converts ChatMessage into a dictionary.

        :returns:
            Serialized version of the object.
        """
        data = asdict(self)
        data["role"] = self.role.value

        if flatten:
            meta = data.pop("meta")
            return {**data, **meta}

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChatMessage":
        """
        Creates a new ChatMessage object from a dictionary.

        :param data:
            The dictionary to build the ChatMessage object.
        :returns:
            The created object.
        """
        data["role"] = ChatRole(data["role"])

        # Store metadata for a moment while we try un-flattening allegedly flatten metadata.
        # We don't expect both a `meta=` keyword and flatten metadata keys so we'll raise a
        # ValueError later if this is the case.
        meta = data.pop("meta", {})
        # Unflatten metadata if it was flattened. We assume any keyword argument that's not
        # a document field is a metadata key. We treat legacy fields as document fields
        # for backward compatibility.
        flatten_meta = {}
        document_fields = [f.name for f in fields(cls)]
        for key in list(data.keys()):
            if key not in document_fields:
                flatten_meta[key] = data.pop(key)

        # We don't support passing both flatten keys and the `meta` keyword parameter
        if meta and flatten_meta:
            raise ValueError(
                "You can pass either the 'meta' parameter or flattened metadata keys as keyword arguments, "
                "but currently you're passing both. Pass either the 'meta' parameter or flattened metadata keys."
            )

        # Finally put back all the metadata
        return cls(**data, meta={**meta, **flatten_meta})
