# ======================================================================
# queries models builder
# ======================================================================
write_cv_queries=False

# CV_POS_SALES_ITEM
# CV_FACT_POS_DISCOUNT
# CV_DIM_POS_SALES_ATTR
# CV_DIM_POS_SALES_ITEM_ATTR
# tab_ws=wb["CV_POS_SALES_ITEM"]
# print(query_builder_from_xls(tab_ws))
if write_cv_queries:
    print("build cv queries")
    cv_list = [wb["CV_FACT_POS_DISCOUNT"], wb["CV_POS_SALES_ITEM"],
           wb["CV_DIM_POS_SALES_ATTR"], wb["CV_DIM_POS_SALES_ITEM_ATTR"]]
    for s in cv_list:

        # -------------- write query into sql file
        mf_path = r"models/CV_queries/"
        mf_name = s.title.lower() + ".sql"
        mf_name = mf_name.replace("/", "_")


        if os.path.exists(mf_path+mf_name):
            os.remove(mf_path+mf_name)
        mf = open(mf_path+mf_name, mode='w', encoding=sys.getdefaultencoding())
        final_query = query_builder_from_xls(s)

        if test == 1:
            print(s.title)
            print(final_query)
        else:
            mf.write(final_query)
