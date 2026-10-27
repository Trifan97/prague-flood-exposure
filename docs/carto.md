# Building the CARTO map (alternative)

> The project's maps are built on Wherobots with kepler.gl: see the README's Maps
> section and `src/prague_flood/maps.py`. This guide is the alternative for anyone who
> wants a CARTO dashboard from the same files; it remains correct.

This guide turns the three GeoParquet files from `python -m prague_flood run` into a CARTO
Builder map with three layers and four widgets. The menu names follow CARTO Builder as
used in October 2026. If a label differs in your workspace, look for the closest
equivalent; the settings themselves are what matter.

The map tells two stories:

- **The defences hold.** 5,421 buildings (108.6 ha of footprint) touch an official flood
  zone. Districts are ranked by that footprint.
- **The defences fail.** Another 4,610 buildings (181.4 ha) lie behind the flood
  defences, mostly in Praha 8, Praha 1 and Praha 7.

## 0. Before you start

1. Run the pipeline from the repository root:

   ```powershell
   . .\scripts\dev_env.ps1
   python -m prague_flood run
   ```

2. Find the three files in `data\processed\`:

   | File | Rows | Geometry | One row per |
   |---|---:|---|---|
   | `prague_flood_districts.parquet` | 57 | MultiPolygon | district, with totals and ranks |
   | `prague_flood_buildings.parquet` | 10,031 | Polygon | exposed or residual-risk building |
   | `prague_flood_zones.parquet` | 2,229 | MultiPolygon | flood zone (whole, not split) |

   All three are in WGS84 (EPSG:4326), which is what CARTO expects.

3. Sign in to your CARTO workspace. It includes a **CARTO Data Warehouse** connection.

## 1. Import the three tables

Repeat for each file:

1. Open **Data Explorer** and click **Import data**.
2. Choose **Local file**, select the file and click **Continue**.
3. Pick the **CARTO Data Warehouse** connection, a folder you can write to, and a table
   name without the extension, for example `prague_flood_districts`.
4. Keep **Let CARTO automatically define the schema** switched on and click **Import**.
5. Open the table preview and check that the geometry column is recognised and that the
   row count matches the table above.

## 2. Create the map and add the sources

1. Create a new map. New accounts show **Create your first map** instead of the usual
   new-map button.
2. Use **Add source from…** three times to add the three tables.
3. Order the layers, from top to bottom: **buildings**, **zones**, **districts**. Drag
   them in the layer list until the buildings draw on top.

## 3. Style the districts layer (exposed footprint)

This is the choropleth that carries the ranking.

1. **Fill → Color:** choose **by exposed_ha**.
2. Pick a sequential palette (light to dark), for example a yellow-to-red ramp.
3. Open the advanced fill options (the sliders icon) and set **Classification** to
   **Custom** with these breaks:

   | Class | exposed_ha |
   |---|---|
   | None | 0 |
   | Low | 0 to 1 |
   | Medium | 1 to 5 |
   | High | 5 to 10 |
   | Very high | 10 and above |

   *Why custom breaks:* 11 of the 57 districts have no exposure, and the rest are very
   skewed (from a few hundred m² up to 18.8 ha). Builder's automatic classes would put
   most districts in one colour.

4. **Opacity** about 70%. **Stroke:** white, 1 px, so district borders stay visible.

To map *how much of each district* is exposed instead, colour by `exposed_share_pct`.
Praha 16 (35%), Lipence (29%) and Velká Chuchle (25%) lead on that measure.

## 4. Style the flood zones layer

1. **Fill → Color:** choose **by zone** (a category).
2. Assign the colours:

   | zone | Colour | Meaning |
   |---|---|---|
   | `q5` | dark blue | Vltava, 5-year flood |
   | `q20` | blue | Vltava, 20-year flood |
   | `q50` | light blue | Vltava, 50-year flood |
   | `q100` | very light blue | Vltava, 100-year flood |
   | `active_zone` | teal | small streams, fastest-flowing part of the Q100 area |
   | `q100_outside_active_zone` | light teal | small streams, rest of the Q100 area |
   | `protected_by_city` | grey | behind the city's flood defences |

3. **Opacity** about 35%. The Vltava zones are nested, so the translucent fills stack
   and the river's most frequent floods come out darkest.
4. **Stroke:** off.

## 5. Style the buildings layer

1. **Fill → Color:** choose **by exposure_class**.
2. Assign the colours:

   | exposure_class | Colour |
   |---|---|
   | `q5` | dark red |
   | `q20` | red |
   | `q50` | orange |
   | `q100` | yellow |
   | `residual` | purple |

3. **Stroke:** off, or 0.5 px dark grey if footprints merge at high zoom.
4. Buildings are small. In the layer's zoom visibility setting, show this layer only
   from about zoom 13, and leave the districts layer to carry the city-wide view.

## 6. Pop-ups (Interactions)

Interactions are off by default. Switch them on per layer.

**Districts, click pop-up**, with these labels:

| Field | Label |
|---|---|
| `district_name` | District |
| `rank_exposed_ha` | Rank by exposed footprint |
| `exposed_ha` | Exposed footprint (ha) |
| `exposed_n` | Exposed buildings |
| `exposed_n_min_size` | Exposed buildings of at least 30 m² |
| `exposed_share_pct` | Share of district footprint exposed (%) |
| `residual_ha` | Footprint behind the defences (ha) |
| `exposed_ml_share_pct` | Exposed buildings that are machine-learned footprints (%) |

**Districts, hover pop-up:** `district_name`, `exposed_ha`, `rank_exposed_ha`. Hover
pop-ups show at most five fields.

**Buildings, hover pop-up:** `exposure_class`, `footprint_m2`, `building_class`,
`source_dataset`, `exposed_by_point`.

Format the areas to one or two decimals.

## 7. Widgets

Add each with **New Widget**, choosing the source and the widget type:

1. **Category**, on the buildings source: column `exposure_class`, operation **Count**.
   This shows how many buildings in the current view fall in each class.
2. **Formula**, on the buildings source: **Sum** of `footprint_m2`. This is the exposed
   footprint in the view, in m². Title it "Footprint in view (m²)".
3. **Category**, on the districts source: column `district_name`, operation **Sum** of
   `exposed_ha`. This gives the ranking as a list. Title it "Exposed footprint by
   district (ha)".
4. **Category**, on the buildings source: column `source_dataset`, operation **Count**.
   This is a data-quality check: how much of what you see rests on machine-learned
   footprints rather than mapped buildings.

Widgets filter to the map view by default. Clicking a class in widget 1 filters the
buildings layer: select `residual` to see the "defences fail" picture on its own.

## 8. Legend

Open the legend from the icons along the left edge of Builder. Check that each layer has
a clear name ("Exposed footprint by district", "Flood zones", "Buildings by most frequent
flood") and that the classes are sorted by **Value**: Q5 first, residual last.

## 9. Basemap, title and description

1. **Basemap:** a light, low-contrast style (for example Positron), so the reds and blues
   carry the meaning.
2. **Title:** "Prague: buildings in official flood zones".
3. **Description** (Markdown):

   ```markdown
   Buildings whose footprint touches an official IPR Praha flood zone (Vltava Q5–Q100
   and small-stream Q100), assuming the city's flood defences hold, plus buildings behind
   the defences (residual risk: flooded only if the defences fail). Districts are ranked
   by exposed building footprint.

   **Not shown:** flash floods from heavy rain, water depths, building values.
   38% of exposed buildings are machine-learned footprints without attributes.

   **Data:** Flood zones and districts: data base © IPR Praha (CC BY 4.0).
   Buildings: © Overture Maps Foundation, © OpenStreetMap contributors,
   Microsoft ML Buildings (ODbL).

   Method and code: <link to the GitHub repository>
   ```

   The attribution is also in `config.ATTRIBUTION`. Keep it: both licences require it.

## 10. Viewer settings and sharing

1. In the map's sharing settings, make the map public or share it with your
   organisation, and switch on the widgets and legend for viewers.
2. Sharing publishes a snapshot, so click **Publish updates** after every later change.

## 11. Final check

Open the public link in a signed-out browser window and check:

- [ ] All three layers draw, buildings on top, and the buildings appear from zoom 13.
- [ ] The district colours show Praha 16, Praha 7, Praha 6 and Praha 4 darkest.
- [ ] Clicking a district shows its pop-up with readable labels and rounded areas.
- [ ] The category widget lists five classes, and clicking `residual` filters the map.
- [ ] The description shows the attribution and the caveats.

## Troubleshooting

- **A label in this guide does not exist.** CARTO renames menus from time to time. Look
  for the setting by its purpose (fill colour by a column, custom classification, layer
  zoom range).
- **Seams inside the flood zones.** You imported the 7,940 join pieces instead of
  `prague_flood_zones.parquet`. The zones file holds whole zones.
- **The map looks empty when zoomed out.** That is the buildings layer's zoom range; the
  districts layer carries the city-wide view.
