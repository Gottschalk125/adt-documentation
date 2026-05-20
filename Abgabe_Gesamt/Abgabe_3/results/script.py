import pandas as pd

# 1. Dateien in der gewünschten Reihenfolge laden
files = {
    "main": "csv_results/main2005.csv",
    "nonpaged": "csv_results/nonpage.csv",
    "sucks": "csv_results/sucksdb.csv",
    "latest": "csv_results/latest_db.csv"
}

dataframes = {}
for name, file in files.items():
    # Wir brauchen nur den Namen des Endpoints und die avg_ms
    df = pd.read_csv(file)[['name', 'avg_ms']]
    # avg_ms runden und leere/fehlende Werte mit 0 füllen
    df['avg_ms'] = df['avg_ms'].fillna(0).round(2) 
    dataframes[name] = df.set_index('name')

# 2. Daten zusammenführen
merged_df = pd.concat(dataframes.values(), axis=1, keys=dataframes.keys())
merged_df.columns = merged_df.columns.droplevel(1)

# 3. LaTeX Code generieren (immer 2 Diagramme pro Zeile)
latex_code = ""
endpoints = merged_df.index.tolist()

for i in range(0, len(endpoints), 2):
    latex_code += "\\noindent\n"
    
    # Linkes Diagramm
    ep1 = endpoints[i]
    vals1 = merged_df.loc[ep1]
    
    # LaTeX-Escaping für Unterstriche im Titel
    ep1_title = ep1.replace("_", "\\_") 
    
    latex_code += f"""\\begin{{minipage}}{{0.48\\textwidth}}
    \\centering
    \\begin{{tikzpicture}}[scale=0.7]
        \\begin{{axis}}[
            ybar,
            bar width=15pt,
            width=\\textwidth,
            height=6cm,
            enlarge x limits=0.2,
            title={{\\textbf{{{ep1_title}}}}},
            ylabel={{Avg. Time (ms)}},
            symbolic x coords={{main, nonpaged, sucks, latest}},
            xtick=data,
            nodes near coords,
            nodes near coords style={{/pgf/number format/.cd, fixed, precision=2}},
            ymin=0
        ]
        \\addplot[fill=keyblue] coordinates {{(main, {vals1['main']}) (nonpaged, {vals1['nonpaged']}) (sucks, {vals1['sucks']}) (latest, {vals1['latest']})}};
        \\end{{axis}}
    \\end{{tikzpicture}}
\\end{{minipage}}%
"""

    # Rechtes Diagramm (falls noch eins übrig ist)
    if i + 1 < len(endpoints):
        ep2 = endpoints[i+1]
        vals2 = merged_df.loc[ep2]
        ep2_title = ep2.replace("_", "\\_")
        
        latex_code += f"""\\hfill%
\\begin{{minipage}}{{0.48\\textwidth}}
    \\centering
    \\begin{{tikzpicture}}[scale=0.7]
        \\begin{{axis}}[
            ybar,
            bar width=15pt,
            width=\\textwidth,
            height=6cm,
            enlarge x limits=0.2,
            title={{\\textbf{{{ep2_title}}}}},
            ylabel={{Avg. Time (ms)}},
            symbolic x coords={{main, nonpaged, sucks, latest}},
            xtick=data,
            nodes near coords,
            nodes near coords style={{/pgf/number format/.cd, fixed, precision=2}},
            ymin=0
        ]
        \\addplot[fill=keyblue] coordinates {{(main, {vals2['main']}) (nonpaged, {vals2['nonpaged']}) (sucks, {vals2['sucks']}) (latest, {vals2['latest']})}};
        \\end{{axis}}
    \\end{{tikzpicture}}
\\end{{minipage}}

\\vspace{{2em}}
"""
    else:
        # Fallback falls ungerade Anzahl
        latex_code += "\n\\vspace{2em}\n"

# In Datei schreiben oder ausgeben
with open("diagrams.tex", "w") as f:
    f.write(latex_code)
print("LaTeX-Code wurde erfolgreich in 'diagrams.tex' generiert!")