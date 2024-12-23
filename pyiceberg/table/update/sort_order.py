from pyiceberg.table import Transaction
from pyiceberg.table.update import UpdateTableMetadata


class SortOrderUpdate(UpdateTableMetadata["SortOrderUpdate"]):
    _transaction = Transaction
    # TODO
