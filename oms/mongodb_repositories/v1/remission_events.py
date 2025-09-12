from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class RemissionEventsRepository(BaseMongoDbRepository):
    def get_by_tracking_id(
        self,
        tracking_id: str,
        *,
        tenant: Optional[List[str]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[int, Iterator[dict]]:
        default_sort = sort
        if default_sort is None:
            default_sort = [("event_timestamp", BaseMongoDbRepository.DESCENDING_ORDER)]
        filter: Dict[str, Any] = {"remission.tracking_id": tracking_id}
        if tenant:
            filter.update({"tenant_id": {"$in": tenant}})
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=default_sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
