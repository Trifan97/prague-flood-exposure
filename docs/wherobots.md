# Running the pipeline on Wherobots

The local pipeline runs on a laptop. This guide runs the same committed code on a
Wherobots runtime, checks that it gives identical results, and times the join.

The Wherobots trial runs for 14 days or $95 of usage, whichever comes first (as of
October 2026). Uploading
files costs nothing; **billing starts only when a notebook runtime is running** (part D)
and stops only when you **Destroy** it (part F). Do parts A to C first, at your own pace.

The button names below come from the Wherobots documentation (October 2026). If your
screen differs, look for the same action. The URLs always work.

**How the files travel.** Everything goes into **My Files**, in one folder next to the
notebook. My Files takes several files per upload, up to 500 MiB each, and appears
directly in JupyterLab. Spark's workers cannot read My Files, though, so the notebook's
cell 5 copies the inputs into **Managed Storage** (S3), where they can. (Uploading the
inputs straight into Storage is not an option: its web upload silently skipped the
39.7 MB buildings file.)

## A. Prepare locally

1. Make sure the work you want to run is committed.
2. Refresh the inputs. Besides downloading, this writes the GeoParquet copies of the IPR
   layers that Spark reads, and checks every file against its pinned SHA-256:

   ```powershell
   . .\scripts\dev_env.ps1
   python -m prague_flood download
   ```

3. Build the code zip:

   ```powershell
   python scripts/package_for_wherobots.py
   ```

   It prints a path like `dist\prague_flood-1a2b3c4.zip`, built with `git archive` from
   that commit (never from uncommitted files). The characters after `prague_flood-` are
   the commit the cloud run uses.

## B. Upload to My Files (three uploads into one folder)

1. Go to **https://cloud.wherobots.com/files**.
2. Expand the region **AWS US West (Oregon)**, then expand **My Files**. Use this region
   throughout: My Files is separate in each region, and Managed Storage is in this one.
3. If there is no `prague-flood` folder yet: click the **Actions** menu (the three
   vertical dots) next to **My Files**, choose **New Folder**, type `prague-flood` and
   click **Create**.
4. **If an older `prague_flood-….zip` is in the folder, delete it.** The notebook stops
   if it finds more than one zip.
5. Click the **Actions** menu next to `prague-flood`, choose **Upload Files**, and upload
   each group below. Select several files with Ctrl. If asked, choose **Replace the
   existing file**.

   | From | Files |
   |---|---|
   | `notebooks\` | `wherobots_run.ipynb` |
   | `dist\` | the zip from part A, step 3 |
   | `data\raw\` | the 8 `.parquet` files (7 starting `ipr_`, and `overture_buildings_prague_2026-10-21.0.parquet`). Not the `.geojson` files. |

## C. Find the S3 folder for the inputs

1. Go to **https://cloud.wherobots.com/storage**. If there is a storage selector at the
   top, make sure it shows **Managed**.
2. Click **data**. Your private folder is the one named **customer-** followed by an ID.
   (Do not use **shared**: everyone in the organisation can see it.)
3. Click the **copy** icon on the right of your `customer-…` row and paste the path into
   Notepad. Add `/prague-flood/raw` to the end. The result looks like
   `s3://…/data/customer-…/prague-flood/raw`. The folder does not need to exist: cell 5
   creates it.

## D. Start a notebook (billing starts here)

1. Go to **https://cloud.wherobots.com** and start a new notebook.
2. **Runtime:** choose the **smallest** size offered. The data is small, and a bigger
   runtime mostly costs more.
3. **Region:** **AWS US West (Oregon)**, the same as part B.
4. **Advanced Options > Idle Timeout:** choose **15** minutes. (It still bills until
   destroyed.)
5. Start it, and wait until the spinning icon turns into a green dot (usually a few
   minutes). If it is still starting after about 5 minutes, destroy it and create it
   again.
6. Click **Open**. JupyterLab opens in a new tab.

## E. Run the notebook

1. In JupyterLab's file browser, open the **MY FILES** panel, then `prague-flood`, and
   double-click `wherobots_run.ipynb`. If asked for a kernel, choose **Python 3**.
2. In the first code cell, replace `s3://PASTE-YOUR-PATH/prague-flood/raw` with the path
   from part C. Keep the quotes.
3. Run everything: menu **Run > Run All Cells**. A running cell shows `[*]`; a finished
   one shows a number.
4. Check three cells as they finish:
   - **Cell 4** prints `code: prague_flood-….zip`.
   - **Cell 5** prints `copied` for all 8 inputs, and **cell 6** prints `ok` for all 8.
   - **Cell 8** prints `identical to the local run with both strategies`.

   If any of them fails, copy the error text, go straight to part F (**Destroy**), and
   see "If something fails" below.
5. Leave `RUN_UNSPLIT_COMPARISON = False`, unless you want to spend up to 10 more
   minutes finding out whether a cluster rescues the unsplit join.

## F. Download, then destroy (billing stops here)

1. In the file browser, open `prague-flood/outputs/`. Right-click each `.parquet` file
   and choose **Download**. They also stay in My Files after the runtime is gone.
2. Save the notebook with its printed results: **File > Download**.
3. Back on the Wherobots console, click **Destroy** on the notebook. Only Destroy stops
   the billing; the idle timeout does not.

## G. Record for the README

- The versions printed by cell 3 (Python, Spark, and the number of cores).
- The timings from cells 7, 8 and 10 (and 9 if you ran it).
- The runtime size you chose, and the usage the console shows for the run.

## H. The maps notebook (for screenshots)

`wherobots_maps.ipynb` renders the three interactive maps (kepler.gl, through
SedonaKepler). Their styling is defined in `src/prague_flood/maps.py`, so you only run
it, take screenshots and destroy the runtime: about 10 minutes of runtime.

1. Upload `notebooks\wherobots_maps.ipynb` to `My Files/prague-flood/`, next to the
   code zip. The inputs from the run notebook are still in `RAW_URI` (Managed Storage
   keeps them), so they do not need uploading again.
2. Start a notebook (part D), open `wherobots_maps.ipynb`, paste the same `RAW_URI` into
   the first cell, and **Run > Run All Cells**.
3. Cell 6 must print `ok` for all 8 inputs. Cell 7 installs kepler.gl's package if the
   runtime lacks it; if a map then stays blank, reload the browser tab (F5) and run that
   map's cell again. The kernel and its data survive a reload.
4. For each map:
   - collapse kepler's side panel with the `<` arrow at its top right;
   - open the legend with the list icon in the column of buttons on the right;
   - take the screenshot (on Windows, Win+Shift+S).
5. Download the three `.html` files from `prague-flood/outputs/maps/`. Each one is the
   interactive map on its own, and opens in any browser.
6. **Destroy** the runtime.

Save the screenshots as `docs/img/map_districts.png`, `docs/img/map_flood_zones.png` and
`docs/img/map_defences_fail.png`, and the README's Maps section can show them.

## If something fails

- **Cell 4: `want one prague_flood-*.zip`.** There are zero or several zips next to the
  notebook. Keep exactly the one from part A.
- **Cell 5 says `not here` and cell 6 says MISSING.** That input was not uploaded next
  to the notebook. Upload it to `My Files/prague-flood/` and run from cell 5 again
  (**Run > Run Selected Cell and All Below**).
- **Cell 6 says DIFFERENT.** The file was changed or comes from another version. Run
  `python -m prague_flood download` locally and upload it again.
- **An access error on `s3://`.** RAW_URI was not pasted completely, or points outside
  your `customer-…` folder. Copy the path again.
- **A `JsonParseException` mentioning a zero byte (`CTRL-CHAR, code 0`).** Something is
  reading `.geojson` instead of `.parquet`. Spark must read only the GeoParquet inputs;
  see `convert.py`.
- **The pages look different from this guide.** Wherobots changes its console from time
  to time. Look for the same actions: create a folder, upload files, copy an S3 path,
  start a notebook, destroy it.
