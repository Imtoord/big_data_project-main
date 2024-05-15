
# Function to get the aggregation queries
def get_aggregation_query(agg_query_name):
    aggregation_queries = {
        "Count Schools by City": [{"$group": {"_id": "$CITY", "count": {"$sum": 1}}}],
        "Average Population by County": [{"$group": {"_id": "$COUNTY", "avg_population": {"$avg": "$POPULATION"}}}],
        "Min and Max Enrollment by State": [{"$group": {"_id": "$STATE", "min_enrollment": {"$min": "$ENROLLMENT"}, "max_enrollment": {"$max": "$ENROLLMENT"}}}],
        "Total Population of All Schools": [{"$group": {"_id": None, "total_population": {"$sum": "$POPULATION"}}}],
        "Top 5 Cities with Most Schools": [
            {"$group": {"_id": "$CITY", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ],
        "Count Schools with Population Greater Than": lambda value: [{"$match": {"POPULATION": {"$gt": value}}}, {"$count": "count"}],
        "Count Schools with Population Less Than": lambda value: [{"$match": {"POPULATION": {"$lt": value}}}, {"$count": "count"}],
        "Count Schools with Enrollment Between": lambda min_val, max_val: [{"$match": {"ENROLLMENT": {"$gt": min_val, "$lt": max_val}}}, {"$count": "count"}],
        "Average Population by State with Population Greater Than": lambda value: [
            {"$match": {"POPULATION": {"$gt": value}}},
            {"$group": {"_id": "$STATE", "avg_population": {"$avg": "$POPULATION"}}}
        ],
        "Average Population by State with Population Less Than": lambda value: [
            {"$match": {"POPULATION": {"$lt": value}}},
            {"$group": {"_id": "$STATE", "avg_population": {"$avg": "$POPULATION"}}}
        ],
        "Average Population by State with Enrollment Between": lambda min_val, max_val: [
            {"$match": {"ENROLLMENT": {"$gt": min_val, "$lt": max_val}}},
            {"$group": {"_id": "$STATE", "avg_population": {"$avg": "$POPULATION"}}}
        ]
    }
    return aggregation_queries[agg_query_name]