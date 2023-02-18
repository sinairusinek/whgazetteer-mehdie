
# Linked People TSV v0.1 (LPeople-TSV)

_18 Feb 2023_

The following TSV people data format will be supported for contributions to the Mehdie system. It is inspired by the LP-TSV, the linked places format, and is provisory. Further discussion is required on date formats, on whether to elaborate the name structures, and mainly on additional fields that will structure the information in the description in properties and relations.
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

URI for the source of the title


**matches**

One or more URIs for matching record(s) in person  authority resources; interpreted as [SKOS:closeMatch](https://www.w3.org/TR/2009/REC-skos-reference-20090818/#L4858), which is "used to link two concepts that are sufficiently similar that they can be used interchangeably in some information retrieval applications" and is inclusive its sub-property SKOS:exactMatch. _semicolon-delimited_

**variants**

One or more name and/or language variants; can be suffixed with language-script codes if available, per IETF best practices, [BCP 47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) using ISO639-1 2-letter codes for language and ISO15924 4-letter codes for script; e.g. {name}@lang-script. **NB** Omit script code if it is the default for a language. _semicolon-delimited_

**type**

We distinguish between individual names (1), collective names (2) and family names (3). Further thought is required on additional types, e.g. deities, fiction, nameless definite descriptions (e.g. "The Sultan of x in year 1000") 

**gender**
female, male, discuss other options

**birth_date**		
Earliest relevant date in ISO 8601 form (YYYY-MM-DD); omit month and/or year as req. BCE years must expressed as negative integer, e.g. -0320 for 320 BCE. To express a range, use a pair of dates, e.g. -0299/-0200 would indicate "born in 3rd century BCE."

**death_date**		
Earliest relevant date in ISO 8601 form (YYYY-MM-DD); omit month and/or year as req. BCE years must expressed as negative integer, e.g. -0320 for 320 BCE. To express a range, use a pair of dates, e.g. -0299/-0200 would indicate "died in 3rd century BCE."

**active_date**		
currently not functional, just there to keep information. 

**birth_place**
String
**birth_place_matches**

**death_place**
**death_place_matches**
**associated_place**
currently not functional, just there to keep information. 


**description**

A short text description of the person that will assist in linking and disambiguation.


### _## future ##_

**other associated_places**
This might be better suited in Json/XML/relational structure than in one flat file. We should find a way to describe places of visit - from a waypoint in a journey, through places X corresponded from, to long residence, as well as places related to roles (x was Jewish Religious leader at Y).
**relation to people**
**fields to document assertion authority and certainty**
**fields to record references**
