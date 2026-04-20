let
    WorkbookPath = #"ExcelUrl",
    PreferredSheetName = "Sheet_Name",
    SparseColumnThreshold = 0.75,

    GetSourceWorkbook = (filePath as text) as table =>
        Excel.Workbook(Web.Contents(filePath), null, true),

    GetFirstVisibleSheet = (source as table, preferredSheetName as text) as table =>
        try
            source{[Item = preferredSheetName, Kind = "Sheet"]}[Data]
        otherwise
            Table.SelectRows(
                source,
                each [Kind] = "Sheet" and ([Hidden] = false or [Hidden] = null)
            ){0}[Data],

    ReplaceErrorsWithNull = (input as table) as table =>
        let
            ColumnNames = Table.ColumnNames(input)
        in
            if List.IsEmpty(ColumnNames) then
                input
            else
                Table.ReplaceErrorValues(
                    input,
                    List.Transform(ColumnNames, (columnName) => {columnName, null})
                ),

    RemoveEmptyRows = (input as table) as table =>
        let
            ColumnNames = Table.ColumnNames(input)
        in
            if List.IsEmpty(ColumnNames) then
                input
            else
                Table.SelectRows(
                    input,
                    each
                        not List.IsEmpty(
                            List.RemoveMatchingItems(Record.FieldValues(_), {null, ""})
                        )
                ),

    RemoveSparseColumns = (input as table, threshold as number) as table =>
        let
            BufferedTable = Table.Buffer(input),
            ColumnNames = Table.ColumnNames(BufferedTable),
            ColumnData =
                if List.IsEmpty(ColumnNames) then
                    {}
                else
                    Table.ToColumns(BufferedTable),
            RowCount =
                if List.IsEmpty(ColumnData) then
                    Table.RowCount(BufferedTable)
                else
                    List.Count(ColumnData{0}),
            NameDataPairs = List.Zip({ColumnNames, ColumnData}),

            SparseColumnsToRemove =
                if RowCount = 0 then
                    {}
                else
                    List.Transform(
                        List.Select(
                            NameDataPairs,
                            (pair) =>
                                (RowCount
                                    - List.NonNullCount(
                                        List.RemoveMatchingItems(pair{1}, {""})
                                    ))
                                / RowCount
                                >= threshold
                        ),
                        (pair) => pair{0}
                    )
        in
            if List.IsEmpty(SparseColumnsToRemove) then
                BufferedTable
            else
                Table.RemoveColumns(BufferedTable, SparseColumnsToRemove),

    PromoteHeadersAndToText = (input as table) as table =>
        if Table.IsEmpty(input) then
            input
        else
            let
                PromotedHeaders =
                    Table.PromoteHeaders(input, [PromoteAllScalars = true]),
                PromotedColumnNames = Table.ColumnNames(PromotedHeaders),
                TextTypedColumns =
                    if List.IsEmpty(PromotedColumnNames) then
                        PromotedHeaders
                    else
                        Table.TransformColumnTypes(
                            PromotedHeaders,
                            List.Transform(
                                PromotedColumnNames,
                                each {_, type text}
                            )
                        )
            in
                TextTypedColumns,

    SourceWorkbook = GetSourceWorkbook(WorkbookPath),
    FirstVisibleSheet = GetFirstVisibleSheet(SourceWorkbook, PreferredSheetName),
    TableWithoutErrors = ReplaceErrorsWithNull(FirstVisibleSheet),
    TableWithoutEmptyRows = RemoveEmptyRows(TableWithoutErrors),
    TableWithoutSparseColumns = RemoveSparseColumns(TableWithoutEmptyRows, SparseColumnThreshold),
    FinalTable = PromoteHeadersAndToText(TableWithoutSparseColumns)
in
    FinalTable