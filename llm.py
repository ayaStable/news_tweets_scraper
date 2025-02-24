from langchain_openai import ChatOpenAI
from environs import Env
from langchain_core.prompts import PromptTemplate
import pandas as pd
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

env = Env()
env.read_env(".env")
gpt_mini = ChatOpenAI(model= "gpt-4o-mini",
                 # max_tokens=4096
                 )
FOOD_SECTOR_MAPPING_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRQXdibXus54aUsemw6_jTqf_BgNXoEfDTNv-QCmyvYRUIGca_e_5M-McIr_45z9oey5pjRMvQUsoT3/pub?gid=1988978843&single=true&output=csv"


def get_category_list():
    df = pd.read_csv(FOOD_SECTOR_MAPPING_URL)
    unique_categories = df[['naic_category', 'Category']].drop_duplicates().to_dict(orient='records')
    return unique_categories


def choose_relevant_niches(scraped_data, batch_size=300):

    # format = """
    # [{{
    #       category: chosen category from the list,
    #       naic_code: corresponding naic code from provided dataframe,
    # }}]
    # """

    prompt_template = """ 
    Your goal is to identify which categories from the provided list are likely to be impacted by the
     market events described in the data. In your analysis, think broadly: include not only manufacturers of 
     affected commodities but also businesses that rely on these commodities as key inputs 
     (for example, bakeries that purchase flour, sugar, and butter, or restaurants and accommodation providers impacted by meat prices, as well as grocery businesses, etc.).
    
    Instructions:
    
    Data Analysis:
    Carefully review the provided scraped data for trends, significant events, and commodity market fluctuations. 
    Identify specific commodities mentioned and analyze their potential impact on market dynamics.
    
    Category Filtering:
    Using the provided list of business categories (each with its NAIC code), identify those categories, while possibly not directly producing commodities, are significantly impacted by their market fluctuations (e.g., businesses that purchase large amounts or depend on these commodities).identify only those categories that are likely to be impacted by the commodity market changes mentioned in the news and tweets.
    Determine which specific commodities (if any) relate to each category’s potential exposure.
    Consider both:
    Direct Impact: Niches that manufacture or directly deal with the affected commodities.
    Indirect Impact: Niches that rely on affected commodities as essential inputs for their operations
    
    Impact Assessment:
    For each identified category, assess the potential impact of the commodity market fluctuations.
    Evaluate the urgency or potential benefit of offering insurance products to that category based on the news/tweets.
    
    Output Structure, return your answer in json format:
    
    Summary of Key Findings: Provide a brief overview of the major insights drawn from the scraped data.
    List of Affected Business categories: For each category from the provided list that is affected, include:
    Business Category Name
    NAIC Code
    Affected Commodities: Specific commodities from the data that are relevant.
    Potential Impact: Explanation of how the commodity market changes could affect the category.
    
    Focus solely on the provided category list and their corresponding NAIC codes, ensuring that your final output is actionable for targeted outreach in a quality market campaign.
    
    Scraped Data: {scrapes}
    Categories List with their NAIC code: {categories}
    """

    prompt = PromptTemplate(template=prompt_template, input_variables=["scrapes", "categories",])
    chain = prompt | gpt_mini.with_structured_output(method="json_mode")

    all_results = []  # To store results from all batches
    categories = get_category_list()
    result = chain.invoke({
        "scrapes": scraped_data,
        "categories": categories,
    })

    # # Process data in batches
    # for i in range(0, total_records, batch_size):
    #     batch = naics_df.iloc[i:i + batch_size].to_dict(orient='records')  # Get batch as list of dicts
    #     logging.info(
    #         f"Processing batch {i // batch_size + 1}/{(total_records // batch_size) + 1} with {len(batch)} records...")
    #
    #     result = chain.invoke({
    #         "niches": batch,
    #         "format": format,
    #         "sector": sector,
    #         "insurance_types": product_type
    #     })
    #
    #     try:
    #         result_list = next(iter(result.values()))
    #         logging.info(f"Batch {i // batch_size + 1} processed. Returned {len(result_list)} items.")
    #         all_results.extend(result_list)
    #     except StopIteration:
    #         all_results.extend(result)

    # Convert results to a dataframe
    # df_merged = pd.DataFrame(all_results)
    try:
        with open("llm_response.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)
        logging.info(f"Data successfully written to llm_response.json")
    except Exception as e:
        logging.error(f"Failed to write data to file: {e}")
    print(result)
    return result