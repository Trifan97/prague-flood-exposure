# Running the pipeline on Wherobots

The local pipeline runs on a laptop. This guide runs the same committed code on a
Wherobots runtime, checks that it gives identical results, and times the join.

The Wherobots trial runs for 14 days or $95 of usage, whichever comes first. Do
everything in part A **before** you start the trial, so the clock only runs during the
actual run (part C). The run itself should take well under an hour of runtime.

## A. Prepare locally (no trial needed)

1. Make sure the work you want to run is committed, then build the code zip:

   ```powershell
   . .\scripts\dev_env.ps1
   python scripts/package_for_wherobots.py
   ```

   It prints a path like `dist\prague_flood-ac79474.zip`. The zip holds `src/prague_flood`
   *as of that commit*, built with `git archive`, never from uncommitted files. Note the
   commit hash: it is the version the cloud run will use.

2. Check that the inputs are all here and verified:

   ```powershell
   python -m prague_flood download   # quick if already downloaded: it only verifies
   ```

   You will upload the 8 files in `data\raw\` (about 80 MB).

3. Keep `notebooks\wherobots_run.ipynb` handy. You will upload it too.

## B. Start the trial and upload (about 10 minutes)

1. Sign up at [wherobots.com](https://wherobots.com/) and start the Professional trial.
   Use your own account and email: this is a personal portfolio project.
2. Open the **Storage** page (cloud.wherobots.com/storage). Your private folder is
   `data/customer-XXXX/`.
3. Inside it, create the folder `prague-flood`, and inside that a folder `raw`.
4. Upload the 8 files from `data\raw\` into `prague-flood/raw/`.
5. Upload the zip from step A1 into `prague-flood/`.
6. Click the **copy** icon on the `prague-flood` folder to copy its S3 path. You will
   paste it into the notebook.

## C. Run (keep it short: the runtime bills while it runs)

1. Start a notebook runtime. Choose the **smallest size offered**: the data is small,
   and a bigger runtime would mostly cost more.
2. In JupyterLab, upload `wherobots_run.ipynb` with the upload button in the file
   browser, or drag it in.
3. In cell 1, paste the S3 path into `PROJECT_URI` (ending in `/prague-flood`), and set
   `CODE_ZIP` to the zip's file name.
4. Run the cells in order.
   - Cell 5 must print `ok` for all 8 inputs.
   - Cell 7 must print `identical to the local run with both strategies`.
   - If either stops with an error, stop the runtime and send the error back.
5. Leave `RUN_UNSPLIT_COMPARISON = False` unless you want to spend up to 10 more minutes
   finding out whether a cluster rescues the unsplit join.
6. Download the three files from `outputs/` (right-click, **Download**), and the notebook
   with its outputs (**File > Download**).
7. **Stop the runtime** in the Wherobots console.

## D. Send back for the README

- The versions printed by cell 3 (Python, Spark, Sedona, and the number of cores).
- The timings from cells 6, 7 and 9 (and 8 if you ran it).
- The runtime size you chose, and the usage the console shows for the run.

The README's comparison then sets these against the laptop numbers: the local Spark run,
and the GeoPandas baseline (`benchmarks/geopandas_baseline.py`).

## If something fails

- **`ModuleNotFoundError: prague_flood`.** The zip path in cell 1 is wrong, or the zip
  is not in `prague-flood/`. Check the name against the Storage page.
- **Cell 5 says MISSING or DIFFERENT.** A file was not uploaded, landed in the wrong
  folder, or was changed. Upload it again from `data\raw\`, which `download` verified.
- **An `ST_...` function is not found.** Wherobots runs its own build of Sedona, and a
  function may differ from Apache Sedona 1.9. Send the error back; there is usually an
  equivalent.
- **The Storage or runtime pages look different from this guide.** Wherobots changes its
  console from time to time. Look for the same actions: upload a file, copy an S3 path,
  start a notebook.
