let
    Source =
        Excel.Workbook(
            File.Contents(
                "C:\Users\willy.kinfoussia.external@spherea.com\OneDrive - SPHEREA\Documents\PowerQuery Project\CA Spherea SP&I\Source\Dim Client&Projet - 03-2026.xlsx"
            ),
            null,
            true
        ),

    // 1) Accès feuille : try inline, pas de variable intermédiaire
    RawSheet =
        try Source{[Item = "Sheet_Name", Kind = "Sheet"]}[Data]
        otherwise
            Table.SelectRows(
                Source,
                each [Kind] = "Sheet" and ([Hidden] = false or [Hidden] = null)
            ){0}[Data],

    // 2) Remplacer toutes les erreurs Excel par null (plus léger que "_error_")
    Colonnes = Table.ColumnNames(RawSheet),
    Remplace = Table.ReplaceErrorValues(
        RawSheet,
        List.Transform(Colonnes, (c) => {c, null})
    ),

    // 3) Supprimer les lignes vides + Buffer unique
    //    → toutes les étapes suivantes lisent depuis ce buffer, sans réévaluation
    Buf = Table.Buffer(
        Table.SelectRows(
            Remplace,
            each List.Count(
                List.Select(Record.FieldValues(_), each _ <> null and _ <> "")
            ) > 0
        )
    ),

    // 4) Analyse des colonnes en UN SEUL PASSAGE via Table.ToColumns
    //    (remplace N appels à Table.Column — chacun retraversait la table entière)
    NbLignes  = Table.RowCount(Buf),
    ColNoms   = Table.ColumnNames(Buf),
    ColData   = if NbLignes = 0 then {} else Table.ToColumns(Buf),

    // 5) Colonnes avec ≥ 75 % de null/vide → à supprimer
    ColonnesASupprimer =
        if NbLignes = 0 then
            {}
        else
            List.Transform(
                List.Select(
                    List.Positions(ColData),
                    (i) =>
                        List.Count(List.Select(ColData{i}, each _ = null or _ = ""))
                        / NbLignes >= 0.75
                ),
                (i) => ColNoms{i}
            ),

    // 6) Suppression des colonnes polluées
    Resultat =
        if List.Count(ColonnesASupprimer) > 0 then
            Table.RemoveColumns(Buf, ColonnesASupprimer)
        else
            Buf
in
    Resultat