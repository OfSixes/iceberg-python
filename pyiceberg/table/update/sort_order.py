from typing import Optional, Union, Any, Callable, Tuple

from pyiceberg.table import Transaction, UpdatesAndRequirements, AddSortOrderUpdate, TableRequirement
from pyiceberg.table.sorting import NullOrder, SortField, SortOrder, SortDirection
from pyiceberg.table.update import UpdateTableMetadata, SetDefaultSortOrderUpdate, AssertDefaultSortOrderId, TableUpdate
from pyiceberg.transforms import Transform
from pyiceberg.types import IcebergType


class UpdateSortOrder(UpdateTableMetadata["UpdateSortOrder"]):
    _transaction = Transaction
    _update_default = bool
    _sort_orders = list[SortOrder]
    _default_sort_order = int
    _new_sort_order_fields = list[SortField]

    def __init__(self, transaction: Transaction, update_default: bool = True) -> None:
        super().__init__(transaction)
        self._update_default = update_default
        self._sort_orders = transaction.table_metadata.sort_orders
        self._default_sort_order = transaction.table_metadata.default_sort_order_id
        self._new_sort_order_fields = []

    def asc(
        self,
        source_id: int,
        transform: Optional[Union[Transform[Any, Any], Callable[[IcebergType], Transform[Any, Any]]]] = None,
        null_order: Optional[NullOrder] = None,
    ):
        new_field = SortField(
            source_id=source_id,
            transform=transform,
            direction=SortDirection.ASC,
            null_order=null_order,
        )
        self._new_sort_order_fields.append(new_field)
        return self

    def desc(
        self,
        source_id: int,
        transform: Optional[Union[Transform[Any, Any], Callable[[IcebergType], Transform[Any, Any]]]] = None,
        null_order: Optional[NullOrder] = None,
    ):
        new_field = SortField(
            source_id=source_id,
            transform=transform,
            direction=SortDirection.DESC,
            null_order=null_order,
        )
        self._new_sort_order_fields.append(new_field)
        return self

    def _commit(self) -> UpdatesAndRequirements:
        new_sort_order = self._apply()
        updates = Tuple[TableUpdate, ...] = ()
        requirements = Tuple[TableRequirement, ...] = ()

        #TODO check if spec already exists
        if self._update_default:
            updates = (
                AddSortOrderUpdate(sort_order=new_sort_order),
                SetDefaultSortOrderUpdate(sort_order_id=-1),
            )
        else:
            updates = (
                AddSortOrderUpdate(sort_order=new_sort_order),
            )

        requirements = None
        return updates, requirements

    def _apply(self) -> SortOrder:
        pass #TODO: see UpdateSpec for inspiration
