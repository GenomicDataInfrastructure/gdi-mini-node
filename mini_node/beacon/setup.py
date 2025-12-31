from logging import getLogger
from urllib.parse import urljoin

from pydantic import HttpUrl

from .config import BeaconContext, BeaconCommonConfig, BeaconComplianceConfig
from .model.common import (
    BeaconQuery,
    BeaconRequest,
    Pagination,
    ReceivedRequestSummary,
    RequestMeta,
    ResponseMeta,
    SchemaPerEntity, BeaconResponse, BeaconQueryResponse,
)
from .model.enums import Granularity, EntityType
from .model.framework.beacon_info import BeaconInfo, BeaconInfoOrganization
from .model.framework.configuration import (
    BeaconConfiguration,
    MaturityAttributes,
    SecurityAttributes,
)
from .model.framework.endpoints_map import BeaconMap, Endpoint
from .model.framework.entry_types import (
    EntryType,
    EntryTypes,
    ReferenceToSchema,
    OntologyTerm,
)
from .model.framework.filtering_term import FilteringTerms
from .model.framework.result_sets import ResultSets, CollectionsList
from .model.framework.service_info import (
    ServiceInfo,
    ServiceInfoType,
    BeaconServiceInfoOrganization,
)
from ..oidc import OidcVerifier
from ..setup import app_version, encode_basic_credential

"""Here are the prepared responses for the Beacon Framework API."""

_log = getLogger(__name__)


def create_beacon_info(context: BeaconContext, base_url: str) -> BeaconInfo:
    info = context.service.info
    organisation = context.common.organisation
    schema_version_tag = context.common.compliance.schemaVersionTag
    base_path = HttpUrl(base_url)

    return BeaconInfo(
        id=info.id,
        name=info.name,
        apiVersion=schema_version_tag,
        environment=info.environment,
        organization=BeaconInfoOrganization(
            id=organisation.id,
            name=organisation.name,
            description=organisation.description,
            address=organisation.address,
            welcomeUrl=organisation.welcomeUrl,
            contactUrl=organisation.contactUrl,
            logoUrl=organisation.logoUrl,
        ),
        description=info.description,
        version=f"v{app_version}",
        welcomeUrl=base_path,
        alternativeUrl=info.alternativeUrl,
        createDateTime=info.createdAt,
        updateDateTime=info.updatedAt,
        info=info.info,
    )


def create_service_info(context: BeaconContext) -> ServiceInfo:
    info = context.service.info
    organisation = context.common.organisation
    version_tag = context.common.compliance.schemaVersionTag

    return ServiceInfo(
        id=info.id,
        name=info.name,
        type=ServiceInfoType(
            artifact="beacon",
            group="org.ga4gh",
            version=version_tag,
        ),
        description=info.description,
        organization=BeaconServiceInfoOrganization(
            name=organisation.name,
            url=organisation.welcomeUrl,
        ),
        contactUrl=organisation.contactUrl,
        documentationUrl=info.documentationUrl,
        createdAt=info.createdAt,
        updatedAt=info.updatedAt,
        environment=info.environment,
        version=f"v{app_version}",
    )


def get_schema_ref(config: BeaconCommonConfig,
                   entity_id: str) -> ReferenceToSchema:
    schema = config.compliance.get_schema(entity_id)
    return ReferenceToSchema(
        id=schema.id,
        name=schema.name,
        description=schema.description,
        referenceToSchemaDefinition=config.compliance.url(schema.path),
    )


def get_schemas(config: BeaconComplianceConfig) -> dict[str, SchemaPerEntity]:
    result = dict()
    for schema in config.schemas:
        result[schema.id] = SchemaPerEntity(
            entityType=schema.id,
            schema=config.url(schema.path),
        )

    # Check that all EntityTypes have their schema defined:
    for entityType in EntityType:
        if entityType not in result:
            raise Exception(f"[configuration] Schema missing for {entityType}")

    return result


def create_endpoint_map(
        config: BeaconCommonConfig,
        permitted_entry_type_ids: set[str],
        base_url: str,
) -> BeaconMap:
    endpoints = dict()
    for entry in config.entryTypes:
        if entry.id not in permitted_entry_type_ids:
            continue
        openapi_url = config.compliance.url(entry.openapi)
        root_url = urljoin(base_url, entry.mainPath.lstrip("/"))

        single_entry_url = None
        if entry.itemPath:
            single_entry_url = str(
                urljoin(base_url, entry.itemPath.lstrip("/")))

        entity_type = EntityType(entry.id)
        endpoints[entity_type] = Endpoint(
            entryType=entity_type,
            openAPIEndpointsDefinition=HttpUrl(openapi_url),
            rootUrl=HttpUrl(root_url),
            singleEntryUrl=single_entry_url,
        )

    return BeaconMap(
        schemaUrl=config.compliance.get_schema_url(EntityType.MAP),
        endpointSets=endpoints,
    )


def create_entry_types(
        config: BeaconCommonConfig, permitted_entry_type_ids: set[str],
) -> EntryTypes:
    result = dict()
    c = config.compliance
    for entry_type in config.entryTypes:
        if entry_type.id not in permitted_entry_type_ids:
            continue
        ontology_term = None
        if entry_type.ontologyTerm:
            t = entry_type.ontologyTerm
            ontology_term = OntologyTerm(id=t.id, label=t.label)
        result[entry_type.id] = EntryType(
            id=entry_type.id,
            name=entry_type.name,
            description=entry_type.description,
            partOfSpecification=c.specification,
            defaultSchema=get_schema_ref(config, entry_type.schemaId),
            ontologyTermForThisType=ontology_term,
        )
    return EntryTypes(entryTypes=result)


def create_configuration(
        context: BeaconContext, entry_types: EntryTypes
) -> BeaconConfiguration:
    params = context.service.configuration
    return BeaconConfiguration(
        schemaUrl=context.common.compliance.get_schema_url(
            EntityType.CONFIGURATION),
        maturityAttributes=MaturityAttributes(
            productionStatus=params.productionStatus,
        ),
        securityAttributes=SecurityAttributes(
            securityLevels=[params.securityLevel],  # We stick to 1 level only.
            defaultGranularity=params.granularity,
        ),
        entryTypes=entry_types.entryTypes,
    )


def create_filtering_terms() -> FilteringTerms:
    # Currently filtering terms are not implemented/supported.
    return FilteringTerms(resources=[], filteringTerms=[])


class BeaconInfoProvider:
    """Helper class that caches BeaconInfo based on incoming URL."""

    def __init__(self, context: BeaconContext):
        self._context = context
        self._cache = dict()

    def resolve(self, base_url: str) -> BeaconInfo:
        if base_url not in self._cache:
            self._cache[base_url] = create_beacon_info(self._context, base_url)
        return self._cache[base_url]


class BeaconMapProvider:
    """Helper class that caches BeaconMap based on incoming URL."""

    def __init__(
            self,
            config: BeaconCommonConfig,
            permitted_entry_type_ids: set[str],
    ):
        self._config = config
        self._cache = dict()
        self._permitted_entry_type_ids = permitted_entry_type_ids

    def resolve(self, base_url: str) -> BeaconMap:
        if base_url not in self._cache:
            self._cache[base_url] = create_endpoint_map(
                self._config, self._permitted_entry_type_ids, base_url)
        return self._cache[base_url]


class BeaconSetup:
    """General Beacon helper-class that aligns API data with the configuration
    values."""

    def __init__(self, context: BeaconContext):
        if context.aggregated:
            # Aggregated Beacon exposes only /datasets and /g_variants:
            visible_entries = {EntityType.DATASET, EntityType.GENOMIC_VARIANT}
        else:
            # Individual-based Beacon exposes only /individuals:
            visible_entries = {EntityType.INDIVIDUAL}

        self._base_path = context.service.base_path
        self._beacon_infos = BeaconInfoProvider(context)
        self._beacon_maps = BeaconMapProvider(context.common, visible_entries)
        self._service_info = create_service_info(context)
        self._entry_types = create_entry_types(context.common, visible_entries)
        self._configuration = create_configuration(context, self._entry_types)
        self._filtering_terms = create_filtering_terms()
        self._response_schemas = get_schemas(context.common.compliance)
        self._hide_lower_counts = context.service.configuration.hideLowerCounts
        self._oidc_verifier = None
        self._basic_headers = None

        oidc_config = context.service.configuration.oidc
        basic_auth_config = context.service.configuration.basic

        if oidc_config is not None and oidc_config.is_effective():
            self._oidc_verifier = OidcVerifier(
                oidc_config.issuer,
                oidc_config.client_id,
                oidc_config.client_secret,
                oidc_config.required_visas,
            )
            self._oidc_verifier.init()
            _log.info(f"[{self._base_path}] OIDC authentication is enforced.")

        if basic_auth_config is not None and len(basic_auth_config) > 0:
            self._basic_headers = set()
            for item in basic_auth_config:
                if item.is_effective():
                    cred = encode_basic_credential(item.username, item.password)
                    self._basic_headers.add(cred)

            if len(self._basic_headers) > 0:
                _log.info(
                    f"[{self._base_path}] Basic authentication is enforced.")
            else:
                self._basic_headers = None

        if self._oidc_verifier and self._basic_headers:
            raise RuntimeError(f"[{self._base_path}] Cannot use both OIDC and "
                               f"Basic auth - configure just one of them")

        if not self._oidc_verifier and not self._basic_headers:
            _log.info(
                f"[{self._base_path}] No user-authentication is enforced.")

    @property
    def base_path(self) -> str:
        return self._base_path

    @property
    def service_info(self) -> ServiceInfo:
        return self._service_info

    @property
    def entry_types(self) -> EntryTypes:
        return self._entry_types

    @property
    def configuration(self) -> BeaconConfiguration:
        return self._configuration

    @property
    def filtering_terms(self) -> FilteringTerms:
        return self._filtering_terms

    def _url_with_path(self, base_url: str) -> str:
        url = urljoin(base_url, self._base_path)
        if not url.endswith("/"):
            url += "/"
        return url

    def censor_count(self, count: int) -> int | None:
        if count is None or count < self._hide_lower_counts:
            return None
        return count

    def beacon_info(self, base_url: str) -> BeaconInfo:
        return self._beacon_infos.resolve(self._url_with_path(base_url))

    def map(self, base_url: str) -> BeaconMap:
        return self._beacon_maps.resolve(self._url_with_path(base_url))

    def request_for_query(
            self,
            query: BeaconQuery | None = None,
            schema: EntityType | None = None,
    ) -> BeaconRequest:
        """Creates the implicit RequestMeta for GET-requests."""
        requested_schemas = []
        if schema is not None:
            requested_schemas = [self._response_schemas[schema]]
        return BeaconRequest(
            meta=RequestMeta(
                apiVersion=self._service_info.type.version,
                requestedSchemas=requested_schemas,
            ),
            query=query or BeaconQuery(),
        )

    def info_response_meta(self, entity_type: EntityType) -> ResponseMeta:
        schema = self._response_schemas[entity_type]
        return ResponseMeta(
            beaconId=self._service_info.id,
            apiVersion=self._service_info.type.version,
            returnedSchemas=[schema],
        )

    def query_response_meta(
            self, request: BeaconRequest, entity_type: EntityType | None,
    ) -> ResponseMeta:
        schema = self._response_schemas.get(entity_type)
        schemas = [schema] if schema is not None else []
        return ResponseMeta(
            beaconId=self._service_info.id,
            apiVersion=self._service_info.type.version,
            returnedSchemas=schemas,
            returnedGranularity=self._granularity(request),
            receivedRequestSummary=self._request_summary(request),
            testMode=request.query.testMode,
        )

    def response(
            self, request: BeaconRequest,
            response: ResultSets | CollectionsList,
            entity_type: EntityType,
    ) -> BeaconResponse:
        if isinstance(response, ResultSets):
            count = len(response.resultSets)
        elif isinstance(response, CollectionsList):
            count = len(response.collections)
        else:
            _log.error(
                f"[{self._base_path}] Unknown response type: {type(response)}")
            count = 0

        summary = BeaconQueryResponse(
            exists=count > 0,
            numTotalResults=self.count_value(request, count),
        )

        return BeaconResponse(
            meta=self.query_response_meta(request, entity_type),
            responseSummary=summary,
            response=self.records_value(request, response),
        )

    def collection_response(
            self, request: BeaconRequest, results: list, entity_type: EntityType
    ) -> BeaconResponse:
        count = len(results)
        summary = BeaconQueryResponse(
            exists=count > 0,
            numTotalResults=self.count_value(request, count),
        )
        collection = None
        if self.is_show_records(request):
            collection = CollectionsList(collections=results)

        return BeaconResponse(
            meta=self.query_response_meta(request, entity_type),
            responseSummary=summary,
            response=collection,
        )

    def _request_summary(
            self,
            request: BeaconRequest,
    ) -> ReceivedRequestSummary | None:
        return ReceivedRequestSummary(
            apiVersion=request.meta.apiVersion,
            requestedSchemas=(request.meta.requestedSchemas or []),
            filters=request.query.filters,
            requestParameters=request.query.requestParameters,
            includeResultsetResponses=request.query.includeResultsetResponses,
            pagination=request.query.pagination or Pagination(),
            requestedGranularity=self._granularity(request),
            testMode=request.query.testMode,
        )

    def _granularity(self, request: BeaconRequest) -> Granularity:
        if request.query.requestedGranularity:
            granularity = request.query.requestedGranularity
        elif self._configuration.securityAttributes.defaultGranularity:
            granularity = self._configuration.securityAttributes.defaultGranularity
        else:
            granularity = Granularity.boolean
        return granularity

    def count_value(self, request: BeaconRequest, count: int) -> int | None:
        visible = self._granularity(request) != Granularity.boolean
        return count if visible else None

    def records_value(self, request: BeaconRequest, records):
        visible = self._granularity(request) == Granularity.record
        return records if visible else None

    def is_show_records(self, request: BeaconRequest) -> bool:
        return self._granularity(request) == Granularity.record

    def authenticate(self, authorization_header: str) -> str | None:
        """Authenticates the given authorization header.

        Returns authentication method ("Basic" / "Bearer") when the header value
        was not accepted. The returned method-string should be used for
        requesting that kind of authentication credential from the user.

        On success, this method returns None.
        """

        if authorization_header is None:
            authorization_header = ""

        if self._oidc_verifier is not None:
            valid = False

            if authorization_header.startswith("Bearer "):
                token = authorization_header.split(" ")[1].strip()
                valid = self._oidc_verifier.verify(token)
            else:
                _log.debug(
                    f"[{self._base_path}] Authorization header is missing from "
                    f"the request or it does not begin with 'Bearer '.")

            if not valid:
                return "Bearer"

        if self._basic_headers is not None:
            if not authorization_header.startswith("Basic "):
                _log.debug(
                    f"[{self._base_path}] Authorization header is missing from "
                    f"the request or it does not begin with 'Basic '."
                )

            valid = authorization_header in self._basic_headers
            _log.debug(
                f"[{self._base_path}] Basic authentication valid: {valid}"
            )
            if not valid:
                return "Basic"

        return None
