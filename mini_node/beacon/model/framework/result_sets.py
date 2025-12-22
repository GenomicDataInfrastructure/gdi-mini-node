from pydantic import BaseModel


class ResultSet(BaseModel):
    id: str
    setType: str = "dataset"
    exists: bool = True
    resultsCount: int
    results: list


class ResultSets(BaseModel):
    resultSets: list[ResultSet] = []


class CollectionsList(BaseModel):
    collections: list = []
