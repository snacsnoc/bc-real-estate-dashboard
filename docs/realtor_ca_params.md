# Realtor.ca search API params

Source: current fetcher implementation in this repo plus observed requests from
Realtor.ca. The API surface can change without notice.

Search endpoint
- `https://api2.realtor.ca/Listing.svc/PropertySearch_Post`
  - HTTP: POST
  - Content-Type: `application/x-www-form-urlencoded`

Search payload fields (POST form params)
- `LatitudeMin`, `LatitudeMax`: bounding box latitude range.
- `LongitudeMin`, `LongitudeMax`: bounding box longitude range.
- `PriceMin`, `PriceMax`: list price range.
- `RecordsPerPage`: results per page.
- `CurrentPage`: 1-based page number.
- `ApplicationId`: usually `1`.
- `CultureId`: usually `1`.
- `Version`: API version string, currently `7.0`.
- `Currency`: usually `CAD`.
- `MaximumResults`: maximum records to return (caps the response).
- `ZoomLevel`: map zoom level (passed through to API).
- `PropertyTypeGroupID`: property group, `1` in our pipeline (residential).
- `PropertySearchTypeId`: search type, `0` in our pipeline.
- `IncludeHiddenListings`: `true` or `false`.
- `TransactionTypeId`: transaction type:
  - `2` = for sale
  - `3` = for rent
- `Sort`: sort key + direction:
  - `1-A` / `1-D` = listing price ascending/descending
  - `6-A` / `6-D` = listing date posted ascending/descending
- `SoldWithinDays`: include sold listings within N days (omit for active listings).
- `NumberOfDays`: listed within N days (optional).

Details endpoint
- `https://api2.realtor.ca/Listing.svc/PropertyDetails` (GET)
- Params:
  - `ApplicationId`
  - `CultureId`
  - `PropertyID`
  - `ReferenceNumber` (MLS number)

Sub-area search endpoint
- `https://api2.realtor.ca/Location.svc/SubAreaSearch` (GET)
- Params:
  - `Area` (string, e.g., "kootenay boundary")
  - `ApplicationId`
  - `CultureId`
  - `Version`
  - `CurrentPage`

Notes
- Listing type IDs are not exposed in the current pipeline. We use
  `PropertyTypeGroupID=1` and `TransactionTypeId` to define the search scope.
