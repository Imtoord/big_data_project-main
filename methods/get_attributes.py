def get_attributes(collection_name):
    if collection_name == "public":
        return [
            "_id", "X", "Y", "FID", "OBJECTID", "NCESID", "NAME", "ADDRESS", 
            "CITY", "STATE", "ZIP", "ZIP4", "TELEPHONE", "TYPE", "STATUS", 
            "POPULATION", "COUNTY", "COUNTYFIPS", "COUNTRY", "LATITUDE", 
            "LONGITUDE", "NAICS_CODE", "NAICS_DESC", "SOURCE", "SOURCEDATE", 
            "VAL_METHOD", "VAL_DATE", "WEBSITE", "LEVEL_", "ENROLLMENT", 
            "START_GRAD", "END_GRADE", "FT_TEACHER", "SHELTER_ID"
        ]
    else:
        return [
            "_id", "X", "Y", "FID", "OBJECTID", "NCESID", "NAME", "ADDRESS", 
            "CITY", "STATE", "ZIP", "ZIP4", "TELEPHONE", "TYPE", "STATUS", 
            "POPULATION", "COUNTY", "COUNTYFIPS", "COUNTRY", "LATITUDE", 
            "LONGITUDE", "NAICS_CODE", "NAICS_DESC", "SOURCE", "SOURCEDATE", 
            "VAL_METHOD", "VAL_DATE", "WEBSITE", "LEVEL_", "ENROLLMENT", 
            "ST_GRADE", "END_GRADE", "DISTRICTID", "FT_TEACHER", "SHELTER_ID"
        ]
