"""
RDF Graph Generator configured by YAML templates (./templates.yaml).

The YAML file has the following structure:

```yaml
namespaces:
  prefix1: url1

templates:
  fairdp:   # template for /fairdp
  catalogs: # template for /catalog
  catalog:  # template for /catalog/*
  dataset:  # template for /dataset/*
  profile:  # template for /profile/*
```

`namespaces` defines common prefixes to be used for given URIs. This mapping
also works vice versa: a prefix can be resolved to a URL.

`templates` defines RDF templates for the entities used in the FDP endpoints.
All templates have the same structure:

```yaml
path:    # string: URL path for this item (used for computing the subject URI)
static:  # dictionary: RDF subjects and their values
mapping: # dictionary: parameter values and corresponding RDF subjects
```

The `path` defines a common absolute path-prefix for all entities of this type.
The full URL is constructed upon an incoming request: base_url + path + optional
entity ID ("id" from mapping parameters).

`static` block defines the default RDF structure for a template, and it resembles
to the RDF triples as YAML `prop: value` is converted into `<predicate> <object>`.

On the other hand, `mapping` defines how input values are included in the graph.
the mappings are defined as `param_name: ns:predicate`. Note that when a
parameter value (input) is None, that specific mapping will be just skipped.

Both `static` and `mapping` blocks support more complex structures, too, which
can complement each other. For example, static block may provide default
predicate values that the `mapping` values can override.

Nested structures are supported using "blank" or anonymous nodes:

```yaml
dct:publisher:
  a: [ foaf:Agent, foaf:Organization ]
  foaf:name: Agent Name
  foaf:homepage: https://www.example.org/
```

Here is a YAML representation of a nested node that is added at dct:publisher,
which has 2 types and 2 properties. Note that the generators automatically
resolves the namespace URIRef for `foaf`, converts "Agent Name" into `Literal`
and "https://www.example.org/" into URIRef. It also supports dates, date-times,
lists, booleans, integers.

Under mapping, a value can be inserted into multiple predicates by defining the
predicates as list. Here, the value of the title parameter is inserted into
dct:title and rdfs:label predicates:

```yaml
title:
  - dct:title
  - rdfs:label
```

Nested structures are supported, and the target predicate(s), where the value
needs to be inserted, is left blank. For example, here the value of
`data_provider_name` is inserted into `dct:publisher` as a nested `foaf:Agent`
node, the provided value itself is inserted with predicates `foaf:name` and
`vcard:fn`, which are left blank in this `mapping` specification:

```yaml
data_provider_name:
  dct:publisher:
    a: foaf:Agent
    foaf:name:
    dcat:contactPoint:
      a: vcard:Kind
      vcard:fn:
    healthdcatap:trustedDataHolder: true
```

To simplify the reuse of configured properties, the templates also support two
kinds of variables:

1. `$FDP_URL` will be substituted with base URL of the FDP service (the path is
   derived from the template: `templates.fairdp.path`). This substitution takes
   place on every request, as the returned hostname depends on request data.
2. `$FDP_CONFIG` enables retrieving property values from the configuration (the
   `fairdp` section). For example, `vcard:hasURL:
   $FDP_CONFIG.contact_point.homepage` will assign the configuration value of
   `fairdp.contact_point.homepage` to the `vcard:hasURL` predicate. This
   substitution takes place only once – when the templates are loaded.

About converting values:
* YAML parser actually converts date and date-time values into Python objects of
  `datetime.date` and `datetime.datetime` if they are properly formatted
  (YYYY-MM-DD for date, YYYY-MM-DD'T'hh:mm:ssZ for date-time).
* rdflib automatically converts the values into ISO-formatted strings.
* some configuration parameters accept only HTTP URLs, so there is a
  type-checking enforced (see `gdi_node_api.setup.model.py`).
* String values that start with `http://`,  `https://`, or `mailto:` are
  converted into URIRef type.
* this generator tries to detect valid email-address values and automatically
  adds a `mailto:` prefix to formalise a valid URIRef.
* integer values are converted into literals with annotation
  `xsd:nonNegativeInteger` if the value is not negative, otherwise `xsd:integer`.

To use this class, just load it once (goes to process templates.yaml) and call
its methods:
1. `item_url(base_url, template_id[, item_id])` to form the subject URL;
2. `render(template_id, base_url[, values[, graph]])` to render a new Graph.

NOTE: the render-method automatically constructs the subject URI. If the values
dictionary also contains an "id" value, it will be appended to the template
path. The "id" value can be provided there even if its template does not define
it under the `mapping` section.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from os.path import dirname, join
from typing import Any
from logging import getLogger

import yaml
from email_validator import validate_email
from pydantic import BaseModel, HttpUrl
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager, RDF, XSD
from rdflib.term import BNode, Literal, URIRef

from mini_node.data import fdp_config

URI_LIKE = re.compile(r"^(https?://|mailto:).*$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_DATETIME = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:?\d{2})?$"
)

_log = getLogger(__name__)

class FdpTemplate(BaseModel):
    """An FDP template is defined by its URL path, static predicates part, and
    mapping part for binding runtime values to predicates."""

    path: str
    static: dict[str, Any]
    mapping: dict[str, Any]


class FdpItems(BaseModel):
    """Templates are provided for the entities listed here."""

    fairdp: FdpTemplate
    catalogs: FdpTemplate
    catalog: FdpTemplate
    dataset: FdpTemplate
    profile: FdpTemplate


class FdpTemplates(BaseModel):
    """Configuration of templates consists of prefix-to-namespace mappings, and
    a collection of entity-specific templates."""

    namespaces: dict[str, str]
    templates: FdpItems


class RDFGraphGenerator:
    """RDFGraphGenerator loads the template configuration from ./templates.yaml
    file and is then ready to render RDF Graphs for specific entities with
    specific customisable parameter-values.
    """

    def __init__(self) -> None:
        """Initialises the generator state from the ./templates.yaml file."""

        with open(join(dirname(__file__), "templates.yaml"), "r") as f:
            data = FdpTemplates(**yaml.safe_load(f))

        self._namespaces: dict[str, Namespace] = {}
        for pfx, iri in data.namespaces.items():
            self._namespaces[pfx] = Namespace(str(iri))
        # Ensure rdf/xsd bound at minimum
        self._namespaces.setdefault("rdf", RDF)
        self._namespaces.setdefault("xsd", XSD)

        self._templates: dict[str, FdpTemplate] = {}
        for key, tmpl in data.templates:
            self._templates[key] = tmpl
            self._apply_config(tmpl.static)
            self._apply_config(tmpl.mapping)
            _log.info("FDP template [%s] is ready.", key)

        self._paths = {k: t.path for k, t in self._templates.items()}

    # -------------------- Public API --------------------
    def base_path(self) -> str:
        """Returns the base URL path of the FAIR Data Point service."""
        return self._paths["fairdp"]

    def item_url(
            self,
            base_url: str,
            tmpl_id: str = "fairdp",
            item_id: str | None = None,
    ) -> str:
        """Constructs a URL using the `path` value specified in a template.

        Args:
            base_url: The base URL of the service (path is excluded).
            tmpl_id: The id of the template to use for resolving path.
            item_id: An optional ID value to be appended.

        Returns:
            constructed URL as string.
        """
        base_path = self._paths[tmpl_id]
        result = base_url.rstrip("/") + "/" + base_path.lstrip("/")
        if item_id is not None:
            result = result.rstrip("/") + "/" + item_id.lstrip("/")
        return result

    def render(
            self,
            template_key: str,
            base_url: str,
            values: dict[str, Any] | None = None,
            graph: Graph | None = None,
    ) -> Graph:
        """Build and return a rdflib.Graph for the given template.

        Args:
            template_key: references a template (by key under `templates`) to be rendered.
            base_url: The base URL of the service (path is excluded).
            values: dictionary providing values for keys referenced in the template's `mapping`.
            graph: Optional existing Graph to include the rendered template.

        Returns:
            The rendered Graph (the passed in graph, otherwise a new instance).
        """
        tmpl = self._require_template(template_key)

        if graph is None:
            graph = Graph()
            ns_mgr = NamespaceManager(graph, bind_namespaces="none")
            for pfx, ns in self._namespaces.items():
                ns_mgr.bind(pfx, ns, override=True, replace=True)
            graph.namespace_manager = ns_mgr

        subj = URIRef(self.item_url(base_url, template_key, values.get("id")))
        fdp_url = self.item_url(base_url)

        # 1) Apply static triples
        self._apply_static(graph, subj, tmpl.static, fdp_url)

        # 2) Apply mappings
        self._apply_mappings(graph, subj, tmpl.mapping, values, fdp_url)

        return graph

    # -------------------- Core helpers --------------------
    def _apply_config(self, yaml_section: dict[str, Any]) -> None:
        """Applies the `fairdp` configuration values to the YAML section where
        the property values start with the `$FDP_CONFIG.` prefix.
        Where the resolved value is None, the property will be removed.
        Otherwise, the value will be replaced by the configuration value.
        """

        for key, value in yaml_section.items():
            if isinstance(value, dict):
                self._apply_config(value)
                continue

            if isinstance(value, list):
                self._apply_config_to_list(key, value)
                continue

            if type(value) != str or not value.startswith("$FDP_CONFIG."):
                continue

            # Update/remove the YAML property:
            resolved_value = self._get_config(key, value)
            if resolved_value is None:
                del yaml_section[key]
            else:
                yaml_section[key] = resolved_value

    def _apply_config_to_list(self, predicate: str, values: list[Any]) -> None:
        for i, value in enumerate(values):
            if isinstance(value, dict):
                self._apply_config(value)
            elif isinstance(value, str) and value.startswith("$FDP_CONFIG."):
                resolved_value = self._get_config(predicate, value)
                values[i] = resolved_value

        if None in values:
            # To effectively remove None values, we need to modify the existing
            # list, not create a new one.
            i = 0
            while i < len(values):
                if values[i] is None:
                    del values[i]
                else:
                    i += 1

    def _get_config(self, predicate, expression: str) -> Any:
        try:
            props = expression.split(".")[1:]
            assert len(props) >= 1, (
                    "Bad/incomplete $FDP_CONFIG expression: " + expression
            )

            # Start retrieving the configuration value:
            resolved_value = fdp_config
            for prop in props:
                assert len(
                    prop) > 0, f"Empty $FDP_CONFIG property in [{expression}]"
                resolved_value = getattr(resolved_value, prop)
            return resolved_value

        except AttributeError as e:
            raise Exception(
                f"Bad $FDP_CONFIG expression for predicate {predicate}:"
                f" error at '{e.name}' in expression '{expression}'"
            )

    def _require_template(self, key: str) -> FdpTemplate:
        try:
            return self._templates[key]
        except KeyError as exc:
            raise KeyError(
                f"Unknown template '{key}'. Available: {list(self._templates)}"
            ) from exc

    def _resolve_qname(self, qname: str) -> URIRef:
        qname = qname.strip()
        if qname == "a":
            return RDF.type
        if ":" not in qname:
            raise ValueError(f"Expected CURIE like 'dct:title', got '{qname}'")
        pfx, local = qname.split(":", 1)
        ns = self._namespaces.get(pfx)
        if ns is None:
            raise KeyError(f"Unknown namespace prefix '{pfx}' in '{qname}'")
        return ns[local]

    def _is_empty_value(self, v: Any) -> bool:
        if v is None:
            return True
        if isinstance(v, str):
            return v.strip() == ""
        if isinstance(v, (list, tuple, set)):
            # empty or all elements empty
            return len(v) == 0 or all(self._is_empty_value(x) for x in v)
        if isinstance(v, dict):
            return len(v) == 0
        return False

    @staticmethod
    def _convert_value(v: Any) -> URIRef | Literal:
        if isinstance(v, datetime):
            return Literal(v.replace(microsecond=0), datatype=XSD.dateTime)

        if isinstance(v, date):
            return Literal(v, datatype=XSD.date)

        if type(v) == int:
            # Prefer xsd:nonNegativeInteger for non-negative integers:
            datatype = XSD.nonNegativeInteger if v >= 0 else XSD.integer
            return Literal(v, datatype=datatype)

        if isinstance(v, HttpUrl):
            return URIRef(v.unicode_string())

        if type(v) == bool:
            return Literal(v, datatype=XSD.boolean)

        if isinstance(v, str):
            s = v.strip()

            # URI-like
            if URI_LIKE.match(s):
                return URIRef(s)

            # Just an email address -> URIRef
            if "@" in s and "." in s and not " " in s:
                if s.endswith("@example.org"):
                    return URIRef("mailto:" + s)
                try:
                    validated = validate_email(s)
                    return URIRef("mailto:" + validated.normalized)
                except Exception as e:
                    _log.warning("Failed to validate email address: %s", s, str(e))
                    pass

            # ISO date or datetime
            if ISO_DATETIME.match(s):
                # Normalize ' ' separator to 'T'
                dt = datetime.fromisoformat(s.replace(" ", "T"))
                return Literal(dt, datatype=XSD.dateTime)
            if ISO_DATE.match(s):
                return Literal(s, datatype=XSD.date)

            return Literal(s)

        # Fallback: treat as literal
        return Literal(v)

    def _set_value(
            self,
            graph: Graph,
            subject: URIRef | BNode,
            predicate: URIRef,
            value: Any,
            fdp_url: str,
            add: bool = False,
    ):
        if value is None:
            return

        # Dict -> nested blank node (filled by dictionary of properties and values)
        if isinstance(value, dict):
            b = None
            if not add:
                for o in graph.objects(subject, predicate, True):
                    b = o
                    break
            if b is None:
                b = BNode()
                graph.add((subject, predicate, b))
            for dk, dv in value.items():
                pred = self._resolve_qname(dk)
                self._set_value(graph, b, pred, dv, fdp_url)
            return

        # List -> multiple values
        if isinstance(value, (list, tuple, set)):
            for item in value:
                self._set_value(graph, subject, predicate, item, fdp_url,
                                add=True)
            return

        if predicate == RDF.type:
            value = self._resolve_qname(value)
            for o in graph.objects(subject, predicate, True):
                if o == value:
                    return
            graph.add((subject, predicate, value))
            return

        # Scalar -> literal/URI
        if type(value) == str and "$FDP_URL" in value:
            value = value.replace("$FDP_URL", fdp_url)

        triple = (subject, predicate, self._convert_value(value))
        graph.add(triple) if add else graph.set(triple)

    # -------------------- Static section --------------------
    def _apply_static(
            self, graph: Graph, subject: URIRef, static: dict[str, Any],
            fdp_url: str
    ):
        for key, val in static.items():
            pred = self._resolve_qname(key)
            self._set_value(graph, subject, pred, val, fdp_url)

    # -------------------- Mapping section --------------------
    def _apply_mappings(
            self,
            graph: Graph,
            subject: URIRef,
            mapping: dict[str, Any],
            values: dict[str, Any],
            fdp_url: str,
    ):
        if values is None or len(values) == 0:
            print("no values")
            return
        if mapping is None or len(mapping) == 0:
            print("no mappings")
            return

        for param_key, path in mapping.items():
            if param_key not in values:
                continue
            val = values[param_key]
            if not self._is_empty_value(val):
                self._apply_mapping_path(graph, subject, path, val, fdp_url)

    def _apply_mapping_path(
            self,
            graph: Graph,
            subject: URIRef | BNode,
            path: str | dict[str, Any],
            value: Any,
            fdp_url: str,
    ):
        if type(path) == str:
            pred = self._resolve_qname(path)
            self._set_value(graph, subject, pred, value, fdp_url)
            return

        if isinstance(path, (list, tuple, set)):
            for item in path:
                self._apply_mapping_path(graph, subject, item, value, fdp_url)
            return

        assert isinstance(path, dict), "Got {}: {}".format(type(path), path)

        for dk, dv in path.items():
            pred = self._resolve_qname(dk)
            # If key doesn't have value, it's where the provided value will be inserted:
            if dv is None:
                self._set_value(graph, subject, pred, value, fdp_url)
                continue

            if type(dv) == str or type(dv) == int or type(dv) == bool:
                self._set_value(graph, subject, pred, dv, fdp_url)
                continue

            # Otherwise, the value is a nested dictionary of properties to be
            # inserted to the new BNode:
            assert isinstance(dv, dict), "Got {}: {}".format(type(dv), dv)
            for o in graph.objects(subject, pred, True):
                b = o
                break
            else:
                b = BNode()
                graph.add((subject, pred, b))
            self._apply_mapping_path(graph, b, dv, value, fdp_url)
