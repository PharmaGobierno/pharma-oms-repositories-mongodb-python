from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class ItemsInventorySnapshotsRepository(BaseMongoDbRepository):
    def get_by_item_sku(
        self,
        sku: str,
        *,
        company: Optional[str] = None,
        project: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[int, Iterator[dict]]:
        filter: Dict[str, Any] = {"item_sku": sku}
        if company:
            filter.update({"company": company})
        if project:
            filter.update({"project": project})
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
