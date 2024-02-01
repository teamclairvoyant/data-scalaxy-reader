# Excel

User needs to add below dependency to the `build.sbt` file:

```Scala
ThisBuild / resolvers += "Github Repo" at "https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-reader/"

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  System.getenv("GITHUB_USERNAME"),
  System.getenv("GITHUB_TOKEN")
)

ThisBuild / libraryDependencies += "com.clairvoyant.data.scalaxy" %% "reader-excel" % "1.0.0"
```

Make sure you add `GITHUB_USERNAME` and `GITHUB_TOKEN` to the environment variables.

`GITHUB_TOKEN` is the Personal Access Token with the permission to read packages.

## API

The library provides below `read` APIs in type class `ExcelToDataFrameReader` in order to parse an Excel file into spark dataframe:

```scala
 def read(
           bytes: Array[Byte],
           excelFormat: ExcelFormat,
           originalSchema: Option[StructType] = None,
           adaptSchemaColumns: StructType => StructType = identity
         ) (using sparkSession: SparkSession): DataFrame
```

The `read` method takes below arguments:

| Argument Name      | Default Value | Description                                                  |
|:-------------------|:-------------:|:-------------------------------------------------------------|
| bytes              |       -       | An Excel file in bytes to be parsed to the dataframe.        |
| excelFormat        |       -       | The `ExcelFormat` representation for the format of the text. |
| originalSchema     |     None      | The schema for the dataframe.                                |
| adaptSchemaColumns |   identity    | The function to modify the inferred schema of the dataframe. |

User can provide below options to the `ExcelFormat` instance:

| Parameter Name                |     Default Value     | Description                                                                                                                                                                                                                                                                                                                                                                                                                            |
|:------------------------------|:---------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| header                        |         true          | Boolean flag to tell whether given excel sheet contains header names or not.                                                                                                                                                                                                                                                                                                                                                           |
| dataAddress                   |          A1           | The location of the data to read from. Following address styles are supported: <br/> `B3:` Start cell of the data. Returns all rows below and all columns to the right. <br/> `B3:F35:` Cell range of data. Reading will return only rows and columns in the specified range. <br/> `'My Sheet'!B3:F35:` Same as above, but with a specific sheet. <br/> `MyTable[#All]:` Table of data. Returns all rows and columns in this table.   |
| treatEmptyValuesAsNulls       |         true          | Treats empty values as null                                                                                                                                                                                                                                                                                                                                                                                                            |
| setErrorCellsToFallbackValues |         false         | If set false errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.                                                                                                                                                                                                                                                                              |
| usePlainNumberFormat          |         false         | If true, format the cells without rounding and scientific notations                                                                                                                                                                                                                                                                                                                                                                    |
| inferSchema                   |         false         | Infers the input schema automatically from data.                                                                                                                                                                                                                                                                                                                                                                                       |
| addColorColumns               |         false         | If it is set to true, adds field with coloured format                                                                                                                                                                                                                                                                                                                                                                                  |
| timestampFormat               | "yyyy-mm-dd hh:mm:ss" | String timestamp format                                                                                                                                                                                                                                                                                                                                                                                                                |
| excerptSize                   |          10           | If set and if schema inferred, number of rows to infer schema from                                                                                                                                                                                                                                                                                                                                                                     |
| maxRowsInMemory               |         None          | If set, uses a streaming reader which can help with big files (will fail if used with xls format files)                                                                                                                                                                                                                                                                                                                                |
| maxByteArraySize              |         None          | See https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-                                                                                                                                                                                                                                                                                                                                   |
| tempFileThreshold             |         None          | Number of bytes at which a zip entry is regarded as too large for holding in memory and the data is put in a temp file instead                                                                                                                                                                                                                                                                                                         |