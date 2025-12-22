from dataclasses import dataclass
from os import listdir
from os.path import dirname, join
from typing import Any

from rdflib import Graph

from ._template import RDFGraphGenerator


@dataclass
class FdpSetup:
    generator: RDFGraphGenerator
    shacls: dict[str, str]

    def render(
        self,
        template_key: str,
        base_url: str,
        values: dict[str, Any] | None = None,
        graph: Graph | None = None,
    ) -> Graph:
        return self.generator.render(template_key, base_url, values, graph)

    def base_url(
        self,
        base_url: str,
        tmpl_id: str = "fairdp",
        item_id: str | None = None,
    ) -> str:
        return self.generator.item_url(base_url, tmpl_id, item_id)

    def shacl(
        self,
        resource_url: str,
        item_id: str | None = None,
    ) -> Graph | None:
        if item_id not in self.shacls:
            return None
        data = f"@prefix : <{resource_url}> .\n{self.shacls[item_id]}"
        return Graph().parse(data=data, format="turtle")


def load_tmpl_config() -> FdpSetup:
    generator = RDFGraphGenerator()

    shacls: dict[str, str] = {}
    shacl_dir = join(dirname(__file__), "shacl")
    for filename in listdir(shacl_dir):
        if not filename.endswith(".ttl"):
            continue
        with open(join(shacl_dir, filename), "r", encoding="utf-8") as file:
            _shacl_id = filename[:-4]
            shacls[_shacl_id] = file.read().strip()

    # NOTE: the SHACL content is stored as string (and not Grpah) as initially
    # the files are not valid – they omit the URL for empty prefix. The missing
    # line is automatically added (constructed from the incoming request URL)
    # when the SHACL file is requested. This approach enables to bind the actual
    # resource URL to the SHACL namespace, which is good for data consistency.

    return FdpSetup(generator=generator, shacls=shacls)
