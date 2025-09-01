# crdt/local_ops_helper.py
from crdt.types import CRDTOperation, OperationType
from crdt.engine import CRDTEngine


class LocalOpsHelper:
    """
    Small wrapper to make local operations (for tests/demos)
    and immediately apply them to the engine.
    """

    def __init__(self, engine: CRDTEngine):
        self.engine = engine

    def make_local_op(self, file_id: str, payload: bytes, op_type: OperationType = OperationType.UPDATE) -> CRDTOperation:
        """
        Create and apply a new local operation on this engine.
        Defaults to UPDATE if not specified.
        """
        return self.engine.apply_local_operation(op_type=op_type, file_id=file_id, payload=payload)
