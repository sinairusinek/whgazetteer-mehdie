
# Linked People TSV v0.1 (LPeople-TSV)

_18 Feb 2023_

The following TSV people data format will be supported for contributions to the Mehdie system. It is modelled after LP-TSV, the linked places format, and is provisory. 
LP-TSV files are unicode text (utf-8). Fields (columns) must be delimited with a tab character. Where multiple values are allowed within a field (indicated below), they are unquoted and delimited with semicolons. 
The following fields will be parsed and converted to Linked Places format automatically upon upload to WHG.

-----

## Fields

### _## required ##_
**id**

Contributor's internal identifier. This must stay consistent throughout accessioning workflow, including subsequent updates

**title**

A single "preferred" person name, which acts as title for the record

**title\_source**

Label or short citation for source of the title name.


### _## encouraged ##_
**title\_uri**

URI for the source of the title toponym

**ccodes**

One or more [ISO Alpha-2 two-letter codes] (https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) for **modern** countries that overlap or cover the place in question. **N.B.** These are used to generate a constraining geometry for searches, and not interpreted as assertions of a relation between the entities.  _semicolon-delimited_

**matches**

One or more URIs for matching record(s) in place name authority resources; interpreted as [SKOS:closeMatch](https://www.w3.org/TR/2009/REC-skos-reference-20090818/#L4858), which is "used to link two concepts that are sufficiently similar that they can be used interchangeably in some information retrieval applications" and is inclusive its sub-property SKOS:exactMatch. _semicolon-delimited_

**variants**

One or more name and/or language variants; can be suffixed with language-script codes if available, per IETF best practices, [BCP 47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) using ISO639-1 2-letter codes for language and ISO15924 4-letter codes for script; e.g. {name}@lang-script. **NB** Omit script code if it is the default for a language. _semicolon-delimited_

**types**

One or more terms for place type (contributor's term, usually verbatim from the source, e.g. pueblo) _semicolon-delimited_


**aat_types**		

One or more AAT integer IDs from WHG's subset list of 160 place type concepts ([tsv](aat_whg-subset.tsv); [xlsx showing hierarchy](aat_whg-subset.xlsx). While not required, this mapping will make records more discoverable in WHG interfaces. NOTE: **aat_types** should correspond to **types**, 1-to-1. If there is no corresponding aat\_type, leave its position empty. E.g. If there are 4 types for a record and only those in positions 2 and 3 have a corresponding aat\_type, this field's value would be something like **1234567;2345678;** indicates  _semicolon-delimited_


### _## optional ##_

**parent_name**

A single toponym for a containing place

**parent_id**

Either 1) a URI for a web-published record of the parent_name above, or 2) a pointer to another record in the same datafile, consisting of a '#' character followed by the **id** of the parent record; e.g. "#1234"

**lon**

Longitude, in decimal degrees

**lat**

Latitude, in decimal degrees

**geowkt**

Any geometry in OGC [WKT format](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry).

>NOTES
>
>- polygons should ideally be simplified to aid rendering performance, using e.g. a GIS function or [MapShaper](https://mapshaper.org/)
>- geowkt will supercede lon/lat pair, if both are supplied; used typically for non-point geometry

**geo_source**

Label for source of the geometry, e.g. GeoNames

**geo_id**

URI identifier for the source of the geometry, e.g.  http://www.geonames.org/2950159

**end**

Latest relevant date, as above; pair indicates ending range

**description**

A short text description of the place

-----
_@kgeographer; 20190819_
