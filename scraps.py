# question 2:
# def find_closest_ic(row, ic_data):
#     hcp_location = row["zip"]
#     closest_ic = None
#     min_distance = float("inf")

#     for ic_index, ic_row in ic_data.iterrows():
#         ic_location = ic_row["zip"]
#         distance = dist.query_postal_code(hcp_location, ic_location)
#         if distance < min_distance:
#             min_distance = distance
#             closest_ic = ic_row

#     closest_ic["dist_hcp_to_ic"] = min_distance
#     return closest_ic

# final_df = pd.DataFrame()

# for ter in ["east", "mid", "west"]:
#     # filter hcp and ic to a specific region
#     filter_hcp = hcp_per_ter.loc[hcp_per_ter["ter"] == ter]
#     filter_ic = ic_per_ter.loc[ic_per_ter["ter"] == ter]

#     # find closest ic for every 'RHEUMATOLOGY', 'NEPHROLOGY' (row in filtered_hcp)
#     filter_hcp[["ter_ic", "account_id", "zip_ic", "dist_hcp_to_ic"]] = filter_hcp.apply(
#         find_closest_ic, ic_data=filter_ic, axis=1
#     )

#     final_df = pd.concat([final_df, filter_hcp])

# print(final_df)


# question 3
# using the data from the previous question, what we have to do to get the 3 closest hcp to an IC
# is we have the data per territory so we have to get the 3 closest hcp's to an IC
# ic_per_ter = duck.query(
#     """
#     with value as (
#         SELECT
#             ter,
#             npi,
#             dist_hcp_to_ic,
#             row_number() OVER (PARTITION BY ter ORDER BY dist_hcp_to_ic) as rn
#         FROM final_df
#         GROUP BY 1,2,3
#     )

#     SELECT ter, array_agg(npi),array_agg(dist_hcp_to_ic) FROM value
#     WHERE rn < 4
#     GROUP BY ter
#         """
# ).to_df()
