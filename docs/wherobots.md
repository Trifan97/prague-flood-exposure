# Running the pipeline on Wherobots

The local pipeline runs on a laptop. This guide runs the same committed code on a
Wherobots runtime, checks that it gives identical results, and times the join.

The Wherobots trial runs for 14 days or $95 of usage, whichever comes first. Uploading
files costs nothing; **billing starts only when a notebook runtime is running** (part D)
and stops only when you **Destroy** it (part F). Do parts A to C first, at your own pace.

The button names below come from the Wherobots documentation (October 2026). If your
screen differs, look for the same action. The URLs always work.

Files go to two different places in Wherobots, for a reason:

| What | Where | Why |
|---|---|---|
| The 8 input files | **Storage** (Managed Storage, an S3 bucket) | Spark's workers read the inputs, and they can read S3 |
| The notebook and the code zip | **My Files** | Only the notebook machine uses them; My Files shows up directly in JupyterLab |

## A. Prepare locally

1. Make sure the work you want to run is committed, then build the code zip:

   ```powershell
   . .\scripts\dev_env.ps1
   python scripts/package_for_wherobots.py
   ```

   It prints a path like `dist\prague_flood-a8daff8.zip`, built with `git archive` from
   that commit (never from uncommitted files). The 8 characters after `prague_flood-` are
   the commit the cloud run will use.

2. Check that the inputs are present and verified (quick when already downloaded):

   ```powershell
   python -m prague_flood download
   ```

## B. Upload the 8 input files (Storage page)

The Storage page uploads **one file at a time**, so this part is 8 short uploads.

1. Go to **https://cloud.wherobots.com/storage** and sign in.
2. If there is a storage selector at the top, make sure it shows **Managed**.
3. Click the **data** folder.
4. Click your personal folder, named **customer-** followed by an ID. Only you can see
   it. (Do not use **shared**: everyone in the organisation can see that one.)
5. Click **Create folder**, name it `prague-flood`, confirm, then click the new
   `prague-flood` folder to open it.
6. Click **Create folder** again, name it `raw`, confirm, and click `raw` to open it. You
   are now in `data/customer-…/prague-flood/raw/`.
7. Upload the first file:
   1. Click **Upload**.
   2. Click **+ Click to Upload a file**. A Windows file dialog opens.
   3. Go to `F:\Wherobots project\prague-flood-exposure\data\raw\` and pick one file.
   4. Click **Upload**, and wait until the file appears in the list.
8. Repeat step 7 for each file, ticking them off:

   | File | Size |
   |---|---:|
   | `ipr_districts.geojson` | 1.7 MB |
   | `ipr_small_streams.geojson` | 23.5 MB |
   | `ipr_vltava_categories.geojson` | 1.5 MB |
   | `ipr_vltava_q5.geojson` | 4.6 MB |
   | `ipr_vltava_q20.geojson` | 3.4 MB |
   | `ipr_vltava_q50.geojson` | 3.0 MB |
   | `ipr_vltava_q100.geojson` | 2.5 MB |
   | `overture_buildings_prague_2026-10-21.0.parquet` | 39.7 MB |

   The page may show sizes in MiB (slightly smaller numbers). That is fine: the notebook
   checks every file's SHA-256 anyway.
9. Copy the folder's S3 path. Go up one level (back to `prague-flood`), find the `raw`
   row, and click the **copy** icon on its right. Paste it into Notepad for now. It
   starts with `s3://` and ends with `/prague-flood/raw`.

## C. Upload the notebook and the code zip (Files page)

This page takes several files at once.

1. Go to **https://cloud.wherobots.com/files**.
2. Expand the region **AWS US West (Oregon)**, then expand **My Files**. Use this region
   throughout: My Files is separate in each region, and Managed Storage is in this one.
3. Click the **Actions** menu (the three vertical dots) next to **My Files**, choose
   **New Folder**, type `prague-flood` and click **Create**.
4. Click the **Actions** menu next to the new `prague-flood` folder and choose
   **Upload Files**.
5. Select both files. Hold Ctrl to select the second one.
   - `F:\Wherobots project\prague-flood-exposure\notebooks\wherobots_run.ipynb`
   - `F:\Wherobots project\prague-flood-exposure\dist\prague_flood-a8daff8.zip`
6. Click **Upload**. Both files now sit in `My Files/prague-flood/`.

## D. Start a notebook (billing starts here)

1. Go to **https://cloud.wherobots.com** and start a new notebook.
2. **Runtime:** choose the **smallest** size offered. The data is small, and a bigger
   runtime mostly costs more.
3. **Name:** optional, for example `prague-flood`.
4. **Region:** **AWS US West (Oregon)**, the same as part C.
5. **Advanced Options > Idle Timeout:** choose **15** minutes, so a forgotten notebook
   stops its kernel sooner. (It still bills until it is destroyed.)
6. Start it, and wait until the spinning icon turns into a green dot. That usually takes
   a few minutes, and billing starts at the green dot. If it is still starting after
   about 5 minutes, destroy it and create it again.
7. Click **Open**. JupyterLab opens in a new tab.

## E. Run the notebook

1. In JupyterLab's file browser on the left, open the **MY FILES** panel, then the
   `prague-flood` folder, and double-click `wherobots_run.ipynb`.
2. If JupyterLab asks for a kernel, choose **Python 3**.
3. In the first code cell, replace `s3://PASTE-YOUR-PATH/prague-flood/raw` with the path
   you copied in part B, step 9. Keep the quotes. Leave `CODE_ZIP` as it is.
4. Run everything: menu **Run > Run All Cells**. The cells run one after the other. A
   cell that is running shows `[*]`; a finished one shows a number.
5. Check two cells as they finish:
   - **Cell 5** must print `ok` for all 8 files.
   - **Cell 7** must print `identical to the local run with both strategies`.

   If either fails with an error, go straight to part F (**Destroy**) and send me the
   error text.
6. Leave `RUN_UNSPLIT_COMPARISON = False`, unless you want to spend up to 10 more
   minutes finding out whether a cluster rescues the unsplit join.

## F. Download, then destroy (billing stops here)

1. In the file browser, open `prague-flood/outputs/`. Right-click each `.parquet` file
   and choose **Download**. They also stay in My Files after the runtime is gone.
2. Save the notebook with its printed results: **File > Download**.
3. Back on the Wherobots console, click **Destroy** on the notebook. Only Destroy stops
   the billing; the idle timeout does not.

## G. Send back for the README

- The versions printed by cell 3 (Python, Spark, Sedona, and the number of cores).
- The timings from cells 6, 7 and 9 (and 8 if you ran it).
- The runtime size you chose, and the usage the console shows for the run.

The README's comparison then sets these against the laptop numbers: the local Spark run,
and the GeoPandas baseline (`benchmarks/geopandas_baseline.py`).

## If something fails

- **`ModuleNotFoundError: prague_flood`.** The notebook and the zip are not in the same
  folder, or `CODE_ZIP` does not match the zip's name. Both must be in
  `My Files/prague-flood/`.
- **Cell 5 says MISSING or DIFFERENT.** A file was not uploaded, landed in the wrong
  folder, or `RAW_URI` points somewhere else. Check that `RAW_URI` ends in
  `/prague-flood/raw`, and upload the file again from `data\raw\`.
- **An access or permission error on `s3://`.** The path was not pasted completely.
  Copy it again with the copy icon.
- **An `ST_...` function is not found.** Wherobots runs its own build of Sedona, and a
  function may differ from Apache Sedona 1.9. Send the error; there is usually an
  equivalent.
- **The pages look different from this guide.** Wherobots changes its console from time
  to time. Look for the same actions: create a folder, upload a file, copy an S3 path,
  start a notebook, destroy it.
