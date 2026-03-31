# RE/MAX listing API params and listing type IDs

Source: reverse-engineered from the RE/MAX Canada Next.js bundles plus the
current fetcher pipeline in this repo. The API surface can change without
notice.

Base endpoint
- `https://api.remax.ca/api/v1/listings/gallery/` (GET, query string params)

Active pins endpoint (compact)
- `https://api.remax.ca/api/v1/listings/active/` (GET, query string params)
- Response `result.results` is a list of semicolon-delimited strings:
  `lat;lng;listingId;listPrice;listingDate;`
- Uses the same bbox + paging params as the gallery endpoint.
- Active-only; no sold listings or full property metadata.

Listing type IDs (from bundle mapping)
- House: `100`, `113`, `122`
- Townhouse: `161`
- Condo: `160`
- Rental: `103`, `130`
- Land: `102`, `180`
- Farm: `106`, `181`
- Duplex: `105`, `155`
- Cottage: `109`, `120`, `150`, `151`, `182`
- Other: `121`, `140`, `170`, `171`
- Commercial categories:
  - Commercial: `101`
  - Business opportunity: `107`
  - Hotel/resort: `108`
  - Commercial lease: `112`

Commercial filter set observed in bundle: `101,107,108,112,102,106,181`

Default exclusions used by the pipeline (commercial + rental): `101,103,107,108,112,130`

Query params
- `from`: result offset (0-based).
- `size`: page size (number of results).
- `zoom`: map zoom level (passed through to API).
- `north`, `south`, `east`, `west`: bounding box coordinates (lat/lon).
- `sortKey`: sort field. Observed values:
  - `1` = listing date (oldest/newest depends on `sortDirection`)
  - `0` = price (lowest/highest depends on `sortDirection`)
- `sortDirection`: sort direction. Observed values:
  - `0` = ascending
  - `1` = descending
- `features.excludedListingTypeIds`: listing type IDs to exclude. This param can
  be repeated multiple times (the fetcher uses repeated params).
- `features.listingTypeIds`: listing type IDs to include (allowlist). When set,
  the UI logic clears `features.excludedListingTypeIds`.
- `features.comingSoon`: include coming soon listings (boolean).
- `features.hasOpenHouse`: open house filter (boolean).
- `features.hasVirtualOpenHouse`: virtual open house filter (boolean).
- `features.isRemaxListing`: RE/MAX listings only (boolean).
- `features.sqFtMin`, `features.sqFtMax`: interior size range.
- `features.lotSizeMin`, `features.lotSizeMax`: lot size range.
- `features.priceListMin`, `features.priceListMax`: list price range.
- `features.pricePerSqFtMin`, `features.pricePerSqFtMax`: price per sqft range.
- `features.bedsMin`, `features.bedsMax`: bedrooms range.
- `features.bathsMin`, `features.bathsMax`: bathrooms range.
- `features.updatedInLastNumDays`: updated/listed within N days.
- `features.storiesMin`, `features.storiesMax`: stories range.
- `features.unitsMin`, `features.unitsMax`: units range.
- `features.totalAcresMin`, `features.totalAcresMax`: acreage range.
- `features.parkingSpacesMin`, `features.parkingSpacesMax`: parking range.
- `features.businessTypeSet`: business type filters (format unclear, likely a
  list of IDs).
- `features.commercialOnly`: commercial listings only (boolean).
- `features.luxuryOnly`: luxury listings only (boolean).
- `features.minImages`: minimum image count.
- `features.featuredLuxury`: featured luxury filter (boolean).

Notes
- The UI uses `features.listingTypeIds` when listing types are explicitly set.
  Otherwise it falls back to `features.excludedListingTypeIds` to remove
  commercial + rental types.
- The `listings/active` endpoint is optimized for map pins and does not include
  property details needed for normalization; use `listings/gallery` for full records.
- Sold filtering appears to be feature-flagged off in the UI (`soldFilterFeature: false`).
- `https://api.remax.ca/api/v1/listings/filters` currently returns only
  `result.businessTypes` (no sold/status fields).
- Feature flags (e.g., `ctech-4670-ca-*`) are served by an Unleash proxy and are
  controlled server-side. The client reads them via a `GET /feature-flags/proxy`
  call and uses `useFlag("<flag-name>")` to branch UI behavior; POST requests
  to `/feature-flags/proxy/client/metrics` only report usage and do not change
  flag state.

Website listings URL query params (from `remax.ca` UI)
These appear in the search page URL (example from `remax.ca-next-bundles/window.js`)
and map into the filter object that is converted to `features.*` params.

Location / paging
- `lang`: language (`en`/`fr`).
- `province`: province slug (e.g., `bc`).
- `city`: city slug (e.g., `creston-real-estate`).
- `pageNumber`: UI page number.
- `sort`: UI sort selection (mapped internally to `sortKey` + `sortDirection`).

Price / size
- `priceMin`, `priceMax` -> `features.priceListMin`, `features.priceListMax`.
- `pricePerSqftMin`, `pricePerSqftMax` -> `features.pricePerSqFtMin`, `features.pricePerSqFtMax`.
- `priceType`: UI price type selector (mapping not observed).
- `sqftMin`, `sqftMax` -> `features.sqFtMin`, `features.sqFtMax`.
- `lotSizeMin`, `lotSizeMax` -> `features.lotSizeMin`, `features.lotSizeMax`.
- `commercialSqftMin`, `commercialSqftMax` -> `features.lotSizeMin` / `features.lotSizeMax`
  when commercial filters are active (see bundle logic).
- `bedsMin`, `bedsMax` -> `features.bedsMin`, `features.bedsMax`.
- `bathsMin`, `bathsMax` -> `features.bathsMin`, `features.bathsMax`.
- `storiesMin`, `storiesMax` -> `features.storiesMin`, `features.storiesMax`.
- `unitsMin`, `unitsMax` -> `features.unitsMin`, `features.unitsMax`.
- `totalAcresMin`, `totalAcresMax` -> `features.totalAcresMin`, `features.totalAcresMax`.
- `parkingSpacesMin`, `parkingSpacesMax` -> `features.parkingSpacesMin`, `features.parkingSpacesMax`.

Listing filters
- `isRemaxListing` -> `features.isRemaxListing`.
- `comingSoon` -> `features.comingSoon`.
- `hasOpenHouse` -> `features.hasOpenHouse`.
- `hasVirtualOpenHouse` -> `features.hasVirtualOpenHouse`.
- `updatedInLastNumDays` -> `features.updatedInLastNumDays`.
- `featuredLuxury` -> `features.featuredLuxury`.
- `minImages` -> `features.minImages`.
- `luxuryOnly` -> `features.luxuryOnly`.
- `commercialOnly` -> `features.commercialOnly`.
- `rentalsOnly` -> influences `features.listingTypeIds` (rental types only).
- `featuredListings`: UI-only flag (mapping not observed in bundle).

Listing type toggles (mapped to listing type IDs)
- `house`, `townhouse`, `condo`, `rental`, `land`, `farm`, `duplex`, `cottage`, `other`.
- Commercial toggles: `commercial`, `commercialLease`, `vacantLand`, `hotelResort`,
  `businessOpportunity`.

Business type toggles (mapped into `features.businessTypeSet`)
- `Agriculture`, `Automotive`, `Construction`, `Grocery`, `Hospitality`, `Hotel`,
  `Industrial`, `Manufacturing`, `Multi-Family`, `Office`, `Professional`,
  `Restaurant`, `Retail`, `Service`, `Transportation`, `Warehouse`.
