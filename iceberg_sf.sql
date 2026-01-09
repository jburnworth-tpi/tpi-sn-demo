

CREATE EXTERNAL VOLUME exvol_lake
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'azure_blob_loc'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://snsnowedgepoc.blob.core.windows.net/snsnowedgepocblob/raw'
        AZURE_TENANT_ID = 'd5b6749c-aa60-406c-8821-26ba318fd2e5'
      )
    )
  ALLOW_WRITES = TRUE;

  DESC EXTERNAL VOLUME exvol_lake;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('exvol_lake');

grant usage on external volume EXVOL_LAKE to role SN_POC_DBT_WRITE_ROLE