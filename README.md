# sql-optimizer-agent
An agent that optimizes sql queries.

## Pre requisites
User must download the TPC-H dataset benchamark manually from https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY

After receiving the email with the link for download, open it and copy the link with the .zip file and create the following env variable:
printf 'TPCH_DOWNLOAD_URL="%s"\n' "<DOWNLOAD_LINK>" > .env
