#!/usr/bin/env python3
"""
Simple CLI script to generate company context for RudderStack Profiles.
Called from rudder-sources main.sh before starting VS Code.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import httpx
from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from constants import TAVILY_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY

logger = logging.getLogger(__name__)


async def search_company_tavily(company_name: str) -> str:
    """Search for company information using Tavily API."""
    if not TAVILY_API_KEY:
        logger.warning("TAVILY_API_KEY not set, skipping web search")
        return f"No web search data available for {company_name}"
    
    try:
        search_query = f"{company_name} business model products services customers data analytics"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": TAVILY_API_KEY,
                    "query": search_query,
                    "search_depth": "advanced",
                    "include_answer": True,
                    "max_results": 5
                },
                timeout=30.0
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                # Format search results into readable text
                search_text = f"Web search results for {company_name}:\n\n"
                for i, result in enumerate(results, 1):
                    search_text += f"{i}. {result.get('title', '')}\n"
                    search_text += f"   {result.get('content', '')}\n\n"
                
                # Add answer if available
                if data.get("answer"):
                    search_text += f"Summary: {data['answer']}\n"
                
                return search_text
            else:
                logger.error(f"Tavily API error: {response.status_code} - {response.text}")
                return f"Web search failed for {company_name}"
                
    except Exception as e:
        logger.error(f"Error searching for {company_name}: {str(e)}")
        return f"Web search error for {company_name}: {str(e)}"


async def generate_context_anthropic(company_name: str, search_results: str, max_retries: int = 3) -> Optional[str]:
    """Generate company context using Anthropic Claude with retry logic."""
    if not ANTHROPIC_API_KEY:
        logger.info("ANTHROPIC_API_KEY not set, skipping Anthropic")
        return None
    
    system_prompt = """You are creating a system prompt for Cline (AI copilot) that will help users build Customer/Creator 360 profiles in RudderStack Profiles for a specific company.

Your output should be a comprehensive system prompt that explains to Cline:

1. What RudderStack Profiles is and how it's used to build customer 360 views
2. The specific company's business model and operations
3. Core entities that should be modeled (customers, products, orders, etc.)
4. Key relationships between entities
5. Identity stitching requirements across different touchpoints
6. Important stakeholders and their roles
7. Marketing objectives and analytics use cases
8. Specific Profiles features that would be valuable for this company
9. Data modeling expectations and output requirements
10. Governance and activation considerations

Write this as a direct instruction to Cline, starting with "You are an AI copilot that builds a Customer/Creator 360 in RudderStack Profiles for [Company Name]..."

Focus on being specific and actionable for building effective customer profiles and analytics for this particular company."""

    user_prompt = f"""Based on this company research, create a comprehensive system prompt for Cline to build RudderStack Profiles for {company_name}:

{search_results}

The system prompt should start with "You are an AI copilot that builds a Customer/Creator 360 in RudderStack Profiles for {company_name}..." and then explain:

- {company_name}'s business model and operations
- Core entities to model in Profiles
- Relationships between entities  
- Identity resolution requirements
- Key stakeholders and their needs
- Marketing/analytics use cases
- Recommended Profiles features
- Data modeling and governance expectations

Make it specific to {company_name}'s business model and industry, focusing on practical guidance for building customer profiles."""

    for attempt in range(max_retries):
        try:
            client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
            
            response = await client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=2000,
                temperature=0.3,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            logger.error(f"Anthropic attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                backoff_time = 2 ** attempt  # 2, 4 seconds backoff
                logger.info(f"Retrying Anthropic in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
            else:
                logger.error("All Anthropic attempts failed")
    
    return None


async def generate_context_openai(company_name: str, search_results: str) -> Optional[str]:
    """Generate company context using OpenAI as backup."""
    if not OPENAI_API_KEY:
        logger.info("OPENAI_API_KEY not set, skipping OpenAI backup")
        return None
    
    try:
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        
        system_prompt = """You are creating a system prompt for Cline (AI copilot) that will help users build Customer 360 profiles in RudderStack Profiles for a specific company.

Your output should be a comprehensive system prompt that explains to Cline the company's business model, core entities to model, identity resolution requirements, key stakeholders, marketing use cases, and recommended Profiles features.

Write this as a direct instruction to Cline, starting with "You are an AI copilot that builds a Customer/Creator 360 in RudderStack Profiles for [Company Name]..." and focusing on practical, actionable guidance specific to that company's business model and industry."""

        user_prompt = f"""Based on this company research, create a comprehensive system prompt for Cline to build RudderStack Profiles for {company_name}:

{search_results}

The system prompt should start with "You are an AI copilot that builds a Customer/Creator 360 in RudderStack Profiles for {company_name}..." and then explain:

- {company_name}'s business model and operations
- Core entities to model in Profiles  
- Identity resolution requirements
- Key stakeholders and their needs
- Marketing/analytics use cases
- Recommended Profiles features
- Data modeling expectations

Make it specific to {company_name}'s business and industry."""

        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"OpenAI backup failed: {str(e)}")
        return None


def create_fallback_context(company_name: str) -> str:
    """Create a basic fallback context when AI services fail."""
    return f"""You are an AI copilot that builds a Customer 360 in RudderStack Profiles for {company_name}.

RudderStack Profiles is a customer data platform that helps you build unified customer profiles by collecting, transforming, and activating customer data from multiple touchpoints. You'll be helping users model entities, resolve identities, and create features for analytics and marketing activation.

Model the following entities and relationships for Profiles, and compute features that power marketing, growth, lifecycle, and analytics use cases:

Core entities: Customer/User, Product/Service, Order/Transaction, Session/Interaction, Campaign/Channel, Support/Issue, and other relevant business entities specific to {company_name}.

Relationships: Customers interact with products/services through various touchpoints; transactions capture purchase behavior; sessions track engagement patterns; campaigns drive acquisition and retention; support interactions indicate satisfaction and issues.

Identity and stitching requirements (RudderStack Profiles):

Use Profiles to resolve identities across known and unknown IDs (anonymous IDs, cookie/device IDs, emails, phone numbers, user IDs) by stitching them into a canonical user using the identity graph and transitive closure, producing a unified rudderId for each person/entity.

Build a transparent, governable identity graph in the warehouse that documents identifier linkages and the provenance of each edge to enable trust, auditability, and explainability for downstream activation.

Key stakeholders and entities:
- Customers/Users: Primary entities for tracking and analysis
- Internal Teams: Marketing, Product, Analytics, Support teams who use Profiles for insights and activation
- Partners/Vendors: External stakeholders that may be relevant to the business model

Marketer objectives and Profiles-driven features:
- Customer segmentation and lifecycle analysis
- Multi-touch attribution and campaign effectiveness measurement
- Personalization and recommendation engines
- Churn prediction and retention programs
- Customer lifetime value optimization and propensity modeling
- Cross-sell and upsell opportunities identification

Data modeling and output expectations:
Produce a Profiles identity graph resolving user identifiers into a canonical rudderId and analogous keys for other entities where applicable.

Materialize feature views for core entities with clear data lineage and appropriate time windows.

Deliver a denormalized Customer 360 table suitable for activation to downstream tools, with freshness SLAs and documented feature definitions.

Activation and governance:
Ensure governance and observability by keeping computation in the warehouse with Profiles, enabling explainability and reproducibility of features and segments.

Prepare audience exports and reverse-ETL mappings keyed on canonical identifiers for safe activation into marketing and CRM destinations.

Note: This is a generic template for {company_name}. Research the company's specific business model, industry, and use cases to provide more tailored guidance for entity modeling, feature engineering, and activation strategies."""


async def create_company_context_file(company_name: str, output_dir: Path = None) -> Path:
    """Main function to create company context file."""
    logger.info(f"Generating company context for {company_name}")
    
    # Set output directory (current directory if not specified)
    if output_dir is None:
        output_dir = Path.cwd()
    
    # 1. Search company using Tavily
    logger.info("Searching company information...")
    search_results = await search_company_tavily(company_name)
    
    # 2. Generate context using Anthropic (with retries)
    logger.info("Generating context with Anthropic...")
    context_content = await generate_context_anthropic(company_name, search_results)
    
    # 3. Fallback to OpenAI if Anthropic fails
    if not context_content:
        logger.info("Falling back to OpenAI...")
        context_content = await generate_context_openai(company_name, search_results)
    
    # 4. Use fallback template if both AI services fail
    if not context_content:
        logger.warning("Both AI services failed, using fallback template")
        context_content = create_fallback_context(company_name)
    
    # 5. Create the context file
    context_file = output_dir / f"{company_name.lower().replace(' ', '_')}_context.md"
    
    # Add header explaining the purpose
    full_content = f"""# RudderStack Profiles Context for {company_name}

This file contains AI-generated context about {company_name} to help you build better RudderStack Profiles projects. This context is used by Cline (AI assistant) to provide company-specific guidance when working with profiles data modeling, identity resolution, and analytics features.

---

{context_content}
"""
    
    with open(context_file, 'w', encoding='utf-8') as f:
        f.write(full_content)
    
    logger.info(f"Created company context file: {context_file}")
    return context_file


def update_clinerules(context_file_path: Path, clinerules_path: Path = None) -> None:
    """Update clinerules.md to reference the company context file."""
    if clinerules_path is None:
        clinerules_path = Path("/home/codeuser/Documents/Cline/Rules/clinerules.md")
    
    # Create directories if they don't exist
    clinerules_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read existing clinerules or create new
    if clinerules_path.exists():
        with open(clinerules_path, 'r', encoding='utf-8') as f:
            existing_content = f.read()
    else:
        existing_content = ""
    
    # Add reference to company context if not already present
    context_reference = f"\n\n# Company Context\nRefer to the company-specific context in: {context_file_path}\nUse this context to understand the business model, entities, and use cases when building RudderStack Profiles projects.\n"
    
    if str(context_file_path) not in existing_content:
        updated_content = existing_content + context_reference
        
        with open(clinerules_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        
        logger.info(f"Updated clinerules.md at {clinerules_path}")
    else:
        logger.info("clinerules.md already contains company context reference")


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate company context for RudderStack Profiles"
    )
    parser.add_argument('company_name', help='Name of the company')
    parser.add_argument('--output-dir', type=Path, help='Output directory for context file')
    parser.add_argument('--clinerules-path', type=Path, help='Path to clinerules.md file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Generate company context file
        context_file = await create_company_context_file(args.company_name, args.output_dir)
        
        # Update clinerules.md
        update_clinerules(context_file, args.clinerules_path)
        
        print(f"✅ Successfully generated company context for {args.company_name}")
        print(f"   Context file: {context_file}")
        print(f"   Updated clinerules.md")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"❌ Failed to generate company context: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())