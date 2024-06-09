import importlib
from typing import Any, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel, ValidationError

from haystack import DeserializationError, Document, component, default_from_dict, default_to_dict, logging
from haystack.document_stores.types import DocumentStore, DuplicatePolicy
from haystack.utils import deserialize_type, serialize_type

logger = logging.getLogger(__name__)


@component
class PydanticDocumentWriter:
    """
    Processes Pydantic BaseModels, transforms them to documents and then writes them to the DocumentStore
    """

    def __init__(
        self, document_store: DocumentStore, model: Type[BaseModel], policy: DuplicatePolicy = DuplicatePolicy.NONE
    ):
        self.document_store = document_store
        self.model = model
        self.policy = policy

    def _get_telemetry_data(self) -> Dict[str, Any]:
        """
        Data that is sent to Posthog for usage analytics.
        """
        return {"document_store": type(self.document_store).__name__}

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the component to a dictionary.

        :returns:
            Dictionary with serialized data.
        """
        return default_to_dict(
            self,
            document_store=self.document_store.to_dict(),
            policy=self.policy.name,
            model=serialize_type(self.model),
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PydanticDocumentWriter":
        """
        Deserializes the component from a dictionary.

        :param data:
            The dictionary to deserialize from.
        :returns:
            The deserialized component.

        :raises DeserializationError:
            If the document store is not properly specified in the serialization data or its type cannot be imported.
        """
        init_params = data.get("init_parameters", {})
        # Load the document_store
        if "document_store" not in init_params:
            raise DeserializationError("Missing 'document_store' in serialization data")
        if "type" not in init_params["document_store"]:
            raise DeserializationError("Missing 'type' in document store's serialization data")

        try:
            module_name, type_ = init_params["document_store"]["type"].rsplit(".", 1)
            logger.debug("Trying to import module '{module_name}'", module_name=module_name)
            module = importlib.import_module(module_name)
        except (ImportError, DeserializationError) as e:
            raise DeserializationError(
                f"DocumentStore of type '{init_params['document_store']['type']}' not correctly imported"
            ) from e

        docstore_class = getattr(module, type_)
        docstore = docstore_class.from_dict(init_params["document_store"])

        # Load the Model
        if "model" not in init_params:
            raise DeserializationError("Missing 'model' in serialization data")

        try:
            modeule_name, type_ = init_params["model"].rsplit(".", 1)
            logger.debug("Trying to import module '{moduel_name}'", modeule_name=modeule_name)
            module = importlib.import_module(module_name)
        except (ImportError, DeserializationError) as e:
            raise DeserializationError(f"BaseModel '{init_params['model']}' not correctly imported") from e

        model = getattr(module, type_)

        data["init_parameters"]["document_store"] = docstore
        data["init_parameters"]["model"] = model
        data["init_parameters"]["policy"] = DuplicatePolicy[data["init_parameters"]["policy"]]
        return default_from_dict(cls, data)

    @component.output_types(documents_written=int)
    def run(self, instances: List[Tuple[BaseModel, Dict[str, Any]]], policy: Optional[DuplicatePolicy] = None):
        """
        Run the PydanticDocumentWriter on the given input data
        """
        if policy is None:
            policy = self.policy

        documents = []
        for inst in instances:
            try:
                v_inst = self.model.model_validate(inst)
            except ValidationError:
                raise ValueError("One of the instances passed did not pass validation.")

            serialized_inst = v_inst.model_dump_json()
            doc = Document()

            print(serialized_inst, doc)

        return documents
