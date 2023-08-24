# text

User need to add below dependency to the `build.sbt` file:

```sbt
ThisBuild / resolvers += "Github Repo" at "https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-reader/"

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  System.getenv("GITHUB_USERNAME"),
  System.getenv("GITHUB_TOKEN")
)

ThisBuild / libraryDependencies += "com.clairvoyant.data.scalaxy" %% "reader-text" % "1.0.0"
```

Make sure you add `GITHUB_USERNAME` and `GITHUB_TOKEN` to the environment variables.

`GITHUB_TOKEN` is the Personal Access Token with the permission to read packages.

User can use this library to read text data of various formats and parse it to spark dataframe.
Supported text formats are:

* CSV
* JSON
* XML

### CSV

Suppose user wants to read CSV text data `csvText` and parse it to spark dataframe.
Then user need to perform below steps:

#### 1. Define file format

```scala
import com.clairvoyant.data.scalaxy.reader.text.formats.CSVTextFormat

val csvTextFormat = CSVTextFormat(
  header = false
)
```

User can provide below options to the `CSVTextFormat` instance:

| Parameter Name                |        Default Value        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| :---------------------------- | :-------------------------: | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| fieldsSeparator               |              ,              | Delimiter by which fields in a row are separated in a csv text.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| charToEscapeQuoteEscaping     |              \              | Sets a single character used for escaping the escape for the quote character.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| columnNameOfCorruptRecord     |       _corrupt_record       | Allows renaming the new field having malformed string created by PERMISSIVE mode. <br/>This overrides `spark.sql.columnNameOfCorruptRecord`.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| comment                       |              #              | Sets a single character used for skipping lines beginning with this character.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| dateFormat                    |         yyyy-MM-dd          | Sets the string that indicates a date format.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| emptyValue                    |      "" (empty string)      | Sets the string representation of an empty value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| enableDateTimeParsingFallback |            true             | Allows falling back to the backward compatible (Spark 1.x and 2.0) behavior of parsing dates and timestamps <br/>if values do not match the set patterns.                                                                                                                                                                                                                                                                                                                                                                                                               |
| encoding                      |            UTF-8            | Decodes the CSV files by the given encoding type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| enforceSchema                 |            true             | If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. <br/>If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. <br/>Field names in the schema and column names in CSV headers are checked by their positions taking into account spark.sql.caseSensitive. <br/>Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results. |
| escape                        |              \              | Sets a single character used for escaping quotes inside an already quoted value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| header                        |            true             | Boolean flag to tell whether csv text contains header names or not.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| inferSchema                   |            true             | Infers the input schema automatically from data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ignoreLeadingWhiteSpace       |            false            | A flag indicating whether or not leading whitespaces from values being read should be skipped.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ignoreTrailingWhiteSpace      |            false            | A flag indicating whether or not trailing whitespaces from values being read should be skipped.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| lineSep                       |             \n              | Defines the line separator that should be used for parsing. Maximum length is 1 character.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| locale                        |            en-US            | Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| maxCharsPerColumn             |             -1              | Defines the maximum number of characters allowed for any given value being read.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |  |
| maxColumns                    |            20480            | Defines a hard limit of how many columns a record can have.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |  |
| mode                          |        FailFastMode         | Allows a mode for dealing with corrupt records during parsing. Allowed values are PermissiveMode, DropMalformedMode and FailFastMode                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| multiLine                     |            false            | Parse one record, which may span multiple lines, per file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| nanValue                      |             NaN             | Sets the string representation of a non-number value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| negativeInf                   |            -Inf             | Sets the string representation of a negative infinity value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| nullValue                     |            null             | Sets the string representation of a null value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| positiveInf                   |             Inf             | Sets the string representation of a positive infinity value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| preferDate                    |            true             | During schema inference (inferSchema), attempts to infer string columns that contain dates as Date if the values satisfy the dateFormat option or default date format. <br/>For columns that contain a mixture of dates and timestamps, try inferring them as TimestampType if timestamp format not specified, otherwise infer them as StringType.                                                                                                                                                                                                                      |
| quote                         |              "              | Sets a single character used for escaping quoted values where the separator can be part of the value. <br/>For reading, if you would like to turn off quotations, you need to set not null but an empty string.                                                                                                                                                                                                                                                                                                                                                         |
| recordSep                     |             \n              | Delimiter by which rows are separated in a csv text.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| samplingRatio                 |             1.0             | Defines fraction of rows used for schema inferring.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| sep                           |              ,              | Delimiter by which fields in a row are separated in a csv text.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| timestampFormat               |     yyyy-MM-dd HH:mm:ss     | Sets the string that indicates a timestamp format.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| timestampNTZFormat            | yyyy-MM-dd'T'HH:mm:ss[.SSS] | Sets the string that indicates a timestamp without timezone format.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| unescapedQuoteHandling        |      STOP_AT_DELIMITER      | Defines how the CsvParser will handle values with unescaped quotes. <br/> Allowed values are STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR                                                                                                                                                                                                                                                                                                                                                                                       |

#### 2. Import type class instance

```scala
import com.clairvoyant.data.scalaxy.reader.text.instances.CSVTextToDataFrameReader
``````

#### 3. Call API

```scala
TextToDataFrameReader
    .read(
        text = csvText,
        textFormat = csvTextFormat
    )
``````

### JSON

Suppose user wants to read JSON text data `jsonText` and parse it to spark dataframe.
Then user need to perform below steps:

#### 1. Define file format

```scala
import com.clairvoyant.data.scalaxy.reader.text.formats.JSONTextFormat

val jsonTextFormat = JSONTextFormat(
  dropFieldIfAllNull = true
)
```

User can provide below options to the `JSONTextFormat` instance:

| Parameter Name                     |        Default Value        | Description                                                                                                                                                |
| :--------------------------------- | :-------------------------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------- |
| allowBackslashEscapingAnyCharacter |            false            | Allows accepting quoting of all character using backslash quoting mechanism.                                                                               |
| allowComments                      |            false            | Ignores Java/C++ style comment in JSON records.                                                                                                            |
| allowNonNumericNumbers             |            true             | Allows JSON parser to recognize set of “Not-a-Number” (NaN) tokens as legal floating number values.                                                        |
| allowNumericLeadingZeros           |            false            | Allows leading zeros in numbers (e.g. 00012).                                                                                                              |
| allowSingleQuotes                  |            true             | Allows single quotes in addition to double quotes.                                                                                                         |
| allowUnquotedControlChars          |            false            | Allows JSON Strings to contain unquoted control characters <br/>(ASCII characters with value less than 32, including tab and line feed characters) or not. |
| allowUnquotedFieldNames            |            false            | Allows unquoted JSON field names.                                                                                                                          |
| columnNameOfCorruptRecord          |       _corrupt_record       | Allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.                      |
| dataColumnName                     |            None             | The name of column that actually contains dataset. If present, the api will only parse dataset of this column to dataframe.                                |
| dateFormat                         |         yyyy-MM-dd          | Sets the string that indicates a date format.                                                                                                              |
| dropFieldIfAllNull                 |            false            | Whether to ignore column of all null values or empty array during schema inference.                                                                        |
| enableDateTimeParsingFallback      |            true             | Allows falling back to the backward compatible (Spark 1.x and 2.0) behavior of parsing dates and timestamps <br/>if values do not match the set patterns.  |
| encoding                           |            UTF-8            | Decodes the CSV files by the given encoding type.                                                                                                          |
| inferSchema                        |            true             | Infers the input schema automatically from data.                                                                                                           |
| lineSep                            |             \n              | Defines the line separator that should be used for parsing. Maximum length is 1 character.                                                                 |
| locale                             |            en-US            | Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.                                        |
| mode                               |        FailFastMode         | Allows a mode for dealing with corrupt records during parsing. Allowed values are PermissiveMode, DropMalformedMode and FailFastMode                       |
| multiLine                          |            false            | Parse one record, which may span multiple lines, per file.                                                                                                 |
| prefersDecimal                     |            false            | Infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.                                   |
| primitivesAsString                 |            false            | Infers all primitive values as a string type.                                                                                                              |
| samplingRatio                      |             1.0             | Defines fraction of rows used for schema inferring.                                                                                                        |
| timestampFormat                    |     yyyy-MM-dd HH:mm:ss     | Sets the string that indicates a timestamp format.                                                                                                         |
| timestampNTZFormat                 | yyyy-MM-dd'T'HH:mm:ss[.SSS] | Sets the string that indicates a timestamp without timezone format.                                                                                        |
| timeZone                           |             UTC             | Sets the string that indicates a time zone ID to be used to format timestamps in the JSON datasources or partition values.                                 |
| originalSchema                     |            None             | Defines the schema for the dataframe.                                                                                                                      |
| adaptSchemaColumns                 |            None             | Defines a function to modify the inferred schema and reapply it to the dataframe.                                                                          |

#### 2. Import type class instance

```scala
import com.clairvoyant.data.scalaxy.reader.text.instances.JSONTextToDataFrameReader
``````

#### 3. Call API

```scala
TextToDataFrameReader
    .read(
        text = jsonText,
        textFormat = jsonTextFormat
    )
``````

### XML

Suppose user wants to read XML text data `xmlText` and parse it to spark dataframe.
Then user need to perform below steps:

#### 1. Define file format

```scala
import com.clairvoyant.data.scalaxy.reader.text.formats.XMLTextFormat

val xmlTextFormat = XMLTextFormat(
  rowTag = "ROW"
)
```

User can provide below options to the `XMLTextFormat` instance:

| Parameter Name            |    Default Value    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| :------------------------ | :-----------------: | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| attributePrefix           |          _          | The prefix for attributes so that we can differentiate attributes and elements.                                                                                                                                                                                                                                                                                                                                                               |
| charset                   |        UTF-8        | Defaults to 'UTF-8' but can be set to other valid charset names.                                                                                                                                                                                                                                                                                                                                                                              |
| columnNameOfCorruptRecord |   _corrupt_record   | Allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.                                                                                                                                                                                                                                                                                                         |
| dateFormat                |     yyyy-MM-dd      | Sets the string that indicates a date format.                                                                                                                                                                                                                                                                                                                                                                                                 |
| excludeAttribute          |        false        | Whether you want to exclude attributes in elements or not.                                                                                                                                                                                                                                                                                                                                                                                    |
| ignoreSurroundingSpaces   |        false        | Defines whether or not surrounding whitespaces from values being read should be skipped.                                                                                                                                                                                                                                                                                                                                                      |
| ignoreNamespace           |        false        | If true, namespaces prefixes on XML elements and attributes are ignored. <br/>Tags <abc:author> and <def:author> would, for example, be treated as if both are just <author>. <br/>Note that, at the moment, namespaces cannot be ignored on the rowTag element, only its children. <br/>Note that XML parsing is in general not namespace-aware even if false.                                                                               |
| inferSchema               |        true         | Infers the input schema automatically from data.                                                                                                                                                                                                                                                                                                                                                                                              |
| mode                      |    FailFastMode     | Allows a mode for dealing with corrupt records during parsing. Allowed values are PermissiveMode, DropMalformedMode and FailFastMode                                                                                                                                                                                                                                                                                                          |
| nullValue                 |        null         | The value to read as null value                                                                                                                                                                                                                                                                                                                                                                                                               |
| rowTag                    |         row         | The row tag of your xml files to treat as a row. For example, in this xml <books> <book><book> ...</books>, the appropriate value would be book.                                                                                                                                                                                                                                                                                              |
| samplingRatio             |         1.0         | Defines fraction of rows used for schema inferring.                                                                                                                                                                                                                                                                                                                                                                                           |
| timestampFormat           | yyyy-MM-dd HH:mm:ss | Sets the string that indicates a timestamp format.                                                                                                                                                                                                                                                                                                                                                                                            |
| valueTag                  |       _VALUE        | The tag used for the value when there are attributes in the element having no child.                                                                                                                                                                                                                                                                                                                                                          |
| wildcardColName           |       xs_any        | Name of a column existing in the provided schema which is interpreted as a 'wildcard'. It must have type string or array of strings. <br/>It will match any XML child element that is not otherwise matched by the schema. The XML of the child becomes the string value of the column. <br/>If an array, then all unmatched elements will be returned as an array of strings. As its name implies, it is meant to emulate XSD's xs:any type. |
| originalSchema            |        None         | Defines the schema for the dataframe.                                                                                                                                                                                                                                                                                                                                                                                                         |
| adaptSchemaColumns        |        None         | Defines a function to modify the inferred schema and reapply it to the dataframe.                                                                                                                                                                                                                                                                                                                                                             |

#### 2. Import type class instance

```scala
import com.clairvoyant.data.scalaxy.reader.text.instances.XMLTextToDataFrameReader
``````

#### 3. Call API

```scala
TextToDataFrameReader
    .read(
        text = xmlText,
        textFormat = xmlTextFormat
    )
``````
