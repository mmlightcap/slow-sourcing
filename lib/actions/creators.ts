"use server"

import { auth } from "@/auth"
import { prisma } from "@/lib/db/prisma"
import Parallel from "parallel-web"

const parallel = new Parallel({ apiKey: process.env.PARALLEL_API_KEY! })

// The shared JSON schema we send to Parallel.ai for structured extraction
const CREATOR_OUTPUT_SCHEMA = {
  type: "json" as const,
  json_schema: {
    type: "object",
    properties: {
      creators: {
        type: "array",
        description: "List of creators found in the search results",
        items: {
          type: "object",
          properties: {
            name: {
              type: "string",
              description: "Full name of the creator",
            },
            bio: {
              type: "string",
              description:
                "Brief description of who they are and what they do (1-2 sentences)",
            },
            category: {
              type: "string",
              description:
                "Content category (e.g. food, fashion, tech, beauty, fitness, lifestyle, finance, gaming, music, art)",
            },
            business_name: {
              type: "string",
              description:
                "Name of their business/brand if mentioned",
            },
            business_description: {
              type: "string",
              description: "What their business does, if mentioned",
            },
            article_title: {
              type: "string",
              description: "Title of the article where they were found",
            },
            article_url: {
              type: "string",
              description: "URL of the article",
            },
            publication: {
              type: "string",
              description:
                "Name of the publication (e.g. Forbes, local newspaper name, blog name)",
            },
            article_date: {
              type: "string",
              description:
                "Date the article was published, in YYYY-MM-DD format if possible, otherwise the best approximation",
            },
            audience_size: {
              type: "string",
              description:
                "Estimated audience/follower count if mentioned (e.g. '50K followers', '1M+ subscribers')",
            },
            social_handles: {
              type: "object",
              description: "Social media handles/usernames if mentioned in the article",
              properties: {
                instagram: { type: "string" },
                tiktok: { type: "string" },
                youtube: { type: "string" },
                twitter: { type: "string" },
              },
            },
          },
          required: ["name", "article_url"],
        },
      },
    },
    required: ["creators"],
  },
}

type ParallelCreator = {
  name: string
  bio?: string
  category?: string
  business_name?: string
  business_description?: string
  article_title?: string
  article_url?: string
  publication?: string
  article_date?: string
  audience_size?: string
  social_handles?: Record<string, string>
}

// Run a Parallel.ai search and return structured creator data
async function runParallelSearch(query: string): Promise<ParallelCreator[]> {
  const run = await parallel.taskRun.create({
    input: `Search the web for: "${query}"

Find articles, profiles, and interviews about social media creators, influencers, and content creators who are building audiences and/or businesses. Search across ALL types of publications — national media, regional newspapers, local outlets, trade publications, blogs, and niche outlets. Cast a wide net. For each creator, extract as much detail as possible: their name, bio, content category, business/brand info, social media handles, audience size, and the article details including its publication date.`,
    processor: "base",
    task_spec: {
      output_schema: CREATOR_OUTPUT_SCHEMA,
    },
  })

  const result = await parallel.taskRun.result(run.run_id, { timeout: 120 })

  if (result.output.type === "json" && result.output.content) {
    const content = result.output.content as { creators?: ParallelCreator[] }
    return content.creators || []
  }
  return []
}

// ══════════════════════════════════════════════════════
// One-off search → saves to Creator pipeline
// ══════════════════════════════════════════════════════

export async function searchCreators(query: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  const searchRun = await prisma.searchRun.create({
    data: { query, status: "running", userId: session.user.id },
  })

  try {
    const creatorsData = await runParallelSearch(query)

    const creators = []
    for (const c of creatorsData) {
      if (!c.name) continue
      const creator = await prisma.creator.create({
        data: {
          name: c.name,
          bio: c.bio || null,
          category: c.category || null,
          businessName: c.business_name || null,
          businessDesc: c.business_description || null,
          articleUrl: c.article_url || null,
          articleTitle: c.article_title || null,
          publication: c.publication || null,
          audienceSize: c.audience_size || null,
          socialHandles: c.social_handles
            ? JSON.parse(JSON.stringify(c.social_handles))
            : undefined,
          searchQuery: query,
          stage: "sourced",
          userId: session.user.id,
        },
      })
      creators.push(creator)
    }

    await prisma.searchRun.update({
      where: { id: searchRun.id },
      data: { status: "completed", results: creators.length },
    })

    return { success: true, creators, searchRunId: searchRun.id }
  } catch (error) {
    await prisma.searchRun.update({
      where: { id: searchRun.id },
      data: { status: "failed" },
    })
    console.error("Search error:", error)
    return { success: false, error: "Search failed. Please try again.", creators: [] }
  }
}

// ══════════════════════════════════════════════════════
// Monitored Queries (always-on article feed)
// ══════════════════════════════════════════════════════

export async function addMonitoredQuery(query: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.monitoredQuery.create({
    data: { query, userId: session.user.id },
  })
}

export async function removeMonitoredQuery(queryId: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  await prisma.monitoredQuery.delete({
    where: { id: queryId, userId: session.user.id },
  })
  return { success: true }
}

export async function getMonitoredQueries() {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.monitoredQuery.findMany({
    where: { userId: session.user.id },
    orderBy: { createdAt: "desc" },
  })
}

// Run ALL active monitored queries and add new article leads
export async function refreshArticleFeed() {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  const queries = await prisma.monitoredQuery.findMany({
    where: { userId: session.user.id, active: true },
  })

  if (queries.length === 0) {
    return { success: true, newArticles: 0, message: "No monitored queries. Add one first!" }
  }

  let totalNew = 0

  // Run all queries in parallel
  const results = await Promise.allSettled(
    queries.map(async (q) => {
      const creatorsData = await runParallelSearch(q.query)

      let newCount = 0
      for (const c of creatorsData) {
        if (!c.name || !c.article_url) continue

        // Deduplicate by article URL
        const existing = await prisma.articleLead.findUnique({
          where: { articleUrl: c.article_url },
        })
        if (existing) continue

        await prisma.articleLead.create({
          data: {
            creatorName: c.name,
            bio: c.bio || null,
            category: c.category || null,
            businessName: c.business_name || null,
            businessDesc: c.business_description || null,
            articleUrl: c.article_url,
            articleTitle: c.article_title || null,
            publication: c.publication || null,
            articleDate: c.article_date || null,
            audienceSize: c.audience_size || null,
            socialHandles: c.social_handles
              ? JSON.parse(JSON.stringify(c.social_handles))
              : undefined,
            searchQuery: q.query,
            user: { connect: { id: session.user!.id! } },
          },
        })
        newCount++
      }

      // Update lastRunAt
      await prisma.monitoredQuery.update({
        where: { id: q.id },
        data: { lastRunAt: new Date() },
      })

      return newCount
    })
  )

  for (const r of results) {
    if (r.status === "fulfilled") totalNew += r.value
  }

  return { success: true, newArticles: totalNew }
}

// Get all article leads
export async function getArticleLeads() {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.articleLead.findMany({
    where: { userId: session.user.id },
    orderBy: { addedAt: "desc" },
  })
}

// Promote an article lead to the Creator pipeline
export async function promoteToCreator(articleId: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  const article = await prisma.articleLead.findUnique({
    where: { id: articleId, userId: session.user.id },
  })
  if (!article) throw new Error("Article not found")

  // Create a Creator from the article lead
  const creator = await prisma.creator.create({
    data: {
      name: article.creatorName,
      bio: article.bio,
      category: article.category,
      businessName: article.businessName,
      businessDesc: article.businessDesc,
      articleUrl: article.articleUrl,
      articleTitle: article.articleTitle,
      publication: article.publication,
      audienceSize: article.audienceSize,
      socialHandles: article.socialHandles
        ? JSON.parse(JSON.stringify(article.socialHandles))
        : undefined,
      searchQuery: article.searchQuery,
      stage: "sourced",
      userId: session.user.id,
    },
  })

  // Mark as promoted
  await prisma.articleLead.update({
    where: { id: articleId },
    data: { promoted: true },
  })

  return creator
}

// Delete an article lead
export async function deleteArticleLead(articleId: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  await prisma.articleLead.delete({
    where: { id: articleId, userId: session.user.id },
  })
  return { success: true }
}

// ══════════════════════════════════════════════════════
// Big Run — FindAll to populate 50-100 creators at once
// ══════════════════════════════════════════════════════

const FINDALL_OBJECTIVE = `Find articles, profiles, and interviews that feature a young person (Gen Z or millennial, roughly ages 18-35) who has built an audience on social media and turned a specific niche passion, skill, craft, or hobby into a real business.

Examples of what we're looking for:
- A young woman who went viral on TikTok teaching needlepoint, then built a canvas and accessories business
- A Gen Z man who posts about sustainable Alaskan fishing and co-founded a salmon company
- A 20-something who shows fashion factory tours on TikTok
- A young founder who built a sold-out adult summer camp experience rooted in community
- A creator who went viral doing blacksmithing and now has a full client roster
- A young birdwatcher who built a conservation nonprofit

The key pattern: social media audience + specific niche + real business (products, services, or organization) — not just brand deals or sponsored content.`

const FINDALL_MATCH_CONDITIONS = [
  {
    name: "has_social_media_audience",
    description:
      "The person featured in the article has built a meaningful audience on at least one social media platform — Instagram, TikTok, YouTube, Twitter/X, or similar. They create content as a core part of their identity, not just occasionally.",
  },
  {
    name: "built_real_business_or_organization",
    description:
      "They have launched an actual business, product line, company, or organization — not just brand deals, sponsored posts, or a general 'influencer' career. Examples: a DTC brand, a physical product, a service business, a nonprofit, an experience company, a retail partnership, a restaurant, or similar.",
  },
  {
    name: "niche_specific_passion",
    description:
      "Their content and business are centered on a specific, identifiable niche, skill, craft, trade, hobby, or passion area — not generic lifestyle, beauty, or fitness content. The niche should be distinct and often unexpected (e.g. needlepoint, blacksmithing, salmon fishing, adult summer camp, birdwatching, fashion manufacturing, mahjong, letter writing, fermentation, woodworking, ceramics, foraging).",
  },
  {
    name: "is_profile_or_interview",
    description:
      "The article is a profile, interview, or feature story focused on this individual creator-founder — not a listicle, trend piece, or roundup where they are one of many names briefly mentioned. The article gives meaningful detail about who they are and what they've built.",
  },
  {
    name: "is_young_founder",
    description:
      "The person is roughly Gen Z or millennial — approximately 18 to 38 years old at the time of the article. The article should mention their age or make clear they are young.",
  },
]

export async function startBigRun(matchLimit: number = 75) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  // Kick off the FindAll job
  const findallRun = await parallel.beta.findall.create({
    entity_type: "article featuring an entrepreneurial creator-founder",
    generator: "base",
    objective: FINDALL_OBJECTIVE,
    match_conditions: FINDALL_MATCH_CONDITIONS,
    match_limit: matchLimit,
  })

  // Build a human-readable summary of the logic used
  const logicSummary = [
    `Objective: ${FINDALL_OBJECTIVE.trim()}`,
    "",
    "Match conditions:",
    ...FINDALL_MATCH_CONDITIONS.map((c) => `• ${c.name}: ${c.description}`),
  ].join("\n")

  // Save to DB so we can poll it
  const bigRun = await prisma.bigRun.create({
    data: {
      findallId: findallRun.findall_id,
      status: "running",
      logic: logicSummary,
      userId: session.user.id,
    },
  })

  return bigRun
}

export async function checkBigRun(bigRunId: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  const bigRun = await prisma.bigRun.findUnique({
    where: { id: bigRunId, userId: session.user.id },
  })
  if (!bigRun) throw new Error("Run not found")

  // Poll Parallel.ai for current status
  const findallRun = await parallel.beta.findall.retrieve(bigRun.findallId)
  const status = findallRun.status.status
  const matchedCount = findallRun.status.metrics.matched_candidates_count ?? 0
  const readCount = findallRun.status.metrics.total_candidates_count ?? 0

  // Update our DB record
  const updated = await prisma.bigRun.update({
    where: { id: bigRunId },
    data: {
      status: status === "completed" ? "completed" : status === "failed" ? "failed" : "running",
      matchedCount,
      readCount,
      completedAt: ["completed", "failed"].includes(status) ? new Date() : undefined,
    },
  })

  return { ...updated, parallelStatus: status, matchedCount }
}

export async function importBigRunResults(bigRunId: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  const bigRun = await prisma.bigRun.findUnique({
    where: { id: bigRunId, userId: session.user.id },
  })
  if (!bigRun) throw new Error("Run not found")

  // Fetch all results from Parallel.ai
  const result = await parallel.beta.findall.result(bigRun.findallId)
  const allCandidates = result.candidates
  const matched = allCandidates.filter((c) => c.match_status === "matched")

  // Save all articles read (matched + unmatched) to articles_read table
  for (const candidate of allCandidates) {
    await prisma.articleRead.upsert({
      where: { id: `${bigRunId}-${Buffer.from(candidate.url).toString("base64").slice(0, 20)}` },
      update: {},
      create: {
        id: `${bigRunId}-${Buffer.from(candidate.url).toString("base64").slice(0, 20)}`,
        bigRunId,
        url: candidate.url,
        title: candidate.name || null,
        description: candidate.description || null,
        matchStatus: candidate.match_status ?? "unknown",
        userId: session.user.id,
      },
    })
  }

  // Update readCount on the big run
  await prisma.bigRun.update({
    where: { id: bigRunId },
    data: { readCount: allCandidates.length },
  })

  // Filter out already-imported URLs first
  const newCandidates = []
  for (const candidate of matched) {
    const existing = await prisma.articleLead.findUnique({
      where: { articleUrl: candidate.url },
    })
    if (!existing) newCandidates.push(candidate)
  }

  // Enrich each new article with full structured data via Task API
  // Run in batches of 5 to avoid overloading
  const BATCH_SIZE = 5
  let imported = 0

  for (let i = 0; i < newCandidates.length; i += BATCH_SIZE) {
    const batch = newCandidates.slice(i, i + BATCH_SIZE)

    const enriched = await Promise.allSettled(
      batch.map(async (candidate) => {
        // Use Task API to extract structured creator info from the article
        const run = await parallel.taskRun.create({
          input: `Extract information about the creator or entrepreneur featured in this article: ${candidate.url}

Article title/description: ${candidate.name} — ${candidate.description || ""}`,
          processor: "base",
          task_spec: {
            output_schema: CREATOR_OUTPUT_SCHEMA,
          },
        })
        const taskResult = await parallel.taskRun.result(run.run_id, { timeout: 60 })

        let creators: ParallelCreator[] = []
        if (taskResult.output.type === "json" && taskResult.output.content) {
          const content = taskResult.output.content as { creators?: ParallelCreator[] }
          creators = content.creators || []
        }

        // Use the first creator found, fall back to FindAll data
        const c = creators[0]
        return {
          candidate,
          creator: c || null,
        }
      })
    )

    for (const res of enriched) {
      const { candidate, creator } = res.status === "fulfilled"
        ? res.value
        : { candidate: batch[enriched.indexOf(res)], creator: null }

      await prisma.articleLead.create({
        data: {
          creatorName: creator?.name || candidate.name,
          bio: creator?.bio || candidate.description || null,
          category: creator?.category || null,
          businessName: creator?.business_name || null,
          businessDesc: creator?.business_description || null,
          articleUrl: candidate.url,
          articleTitle: creator?.article_title || candidate.name,
          publication: extractPublication(candidate.url),
          articleDate: creator?.article_date || null,
          audienceSize: creator?.audience_size || null,
          socialHandles: creator?.social_handles
            ? JSON.parse(JSON.stringify(creator.social_handles))
            : undefined,
          searchQuery: "Big Run (FindAll)",
          user: { connect: { id: session.user.id! } },
        },
      })
      imported++
    }
  }

  await prisma.bigRun.update({
    where: { id: bigRunId },
    data: { importedCount: imported },
  })

  return { success: true, imported, total: matched.length }
}

// Helper: extract publication name from URL
function extractPublication(url: string): string {
  try {
    const hostname = new URL(url).hostname.replace("www.", "")
    const knownPubs: Record<string, string> = {
      "nytimes.com": "New York Times",
      "washingtonpost.com": "Washington Post",
      "forbes.com": "Forbes",
      "businessinsider.com": "Business Insider",
      "fortune.com": "Fortune",
      "voguebusiness.com": "Vogue Business",
      "vogue.com": "Vogue",
      "food52.com": "Food52",
      "techcrunch.com": "TechCrunch",
      "theverge.com": "The Verge",
      "bloomberg.com": "Bloomberg",
      "cnbc.com": "CNBC",
      "buzzfeed.com": "BuzzFeed",
      "huffpost.com": "HuffPost",
      "medium.com": "Medium",
      "glossy.co": "Glossy",
      "digiday.com": "Digiday",
      "adweek.com": "Adweek",
      "entrepreneur.com": "Entrepreneur",
      "inc.com": "Inc.",
      "fastcompany.com": "Fast Company",
      "wired.com": "Wired",
      "wsj.com": "Wall Street Journal",
      "theatlantic.com": "The Atlantic",
      "refinery29.com": "Refinery29",
      "buzzfeednews.com": "BuzzFeed News",
      "popsugar.com": "PopSugar",
      "wellandgood.com": "Well+Good",
      "who.com.au": "WHO Australia",
    }
    return knownPubs[hostname] || hostname
  } catch {
    return "Unknown"
  }
}

// Re-enrich existing article leads that are missing data
export async function reenrichArticleLeads() {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  // Find leads that are missing key data
  const stale = await prisma.articleLead.findMany({
    where: {
      userId: session.user.id,
      OR: [
        { publication: "Unknown" },
        { publication: null },
        { creatorName: { equals: "" } },
      ],
    },
  })

  if (stale.length === 0) return { success: true, enriched: 0 }

  const BATCH_SIZE = 5
  let enriched = 0

  for (let i = 0; i < stale.length; i += BATCH_SIZE) {
    const batch = stale.slice(i, i + BATCH_SIZE)

    const results = await Promise.allSettled(
      batch.map(async (article) => {
        const run = await parallel.taskRun.create({
          input: `Extract information about the creator or entrepreneur featured in this article: ${article.articleUrl}`,
          processor: "base",
          task_spec: { output_schema: CREATOR_OUTPUT_SCHEMA },
        })
        const taskResult = await parallel.taskRun.result(run.run_id, { timeout: 60 })

        let creators: ParallelCreator[] = []
        if (taskResult.output.type === "json" && taskResult.output.content) {
          const content = taskResult.output.content as { creators?: ParallelCreator[] }
          creators = content.creators || []
        }
        return { article, creator: creators[0] || null }
      })
    )

    for (const res of results) {
      if (res.status !== "fulfilled") continue
      const { article, creator } = res.value
      if (!creator) {
        // At minimum fix the publication from the URL
        await prisma.articleLead.update({
          where: { id: article.id },
          data: { publication: extractPublication(article.articleUrl) },
        })
        enriched++
        continue
      }

      await prisma.articleLead.update({
        where: { id: article.id },
        data: {
          creatorName: creator.name || article.creatorName,
          bio: creator.bio || article.bio,
          category: creator.category || article.category,
          businessName: creator.business_name || article.businessName,
          businessDesc: creator.business_description || article.businessDesc,
          articleTitle: creator.article_title || article.articleTitle,
          publication: extractPublication(article.articleUrl),
          articleDate: creator.article_date || article.articleDate,
          audienceSize: creator.audience_size || article.audienceSize,
          socialHandles: creator.social_handles
            ? JSON.parse(JSON.stringify(creator.social_handles))
            : article.socialHandles ?? undefined,
        },
      })
      enriched++
    }
  }

  return { success: true, enriched, total: stale.length }
}

export async function getBigRuns() {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.bigRun.findMany({
    where: { userId: session.user.id },
    orderBy: { createdAt: "desc" },
    take: 10,
  })
}

// ══════════════════════════════════════════════════════
// Creator pipeline CRUD
// ══════════════════════════════════════════════════════

export async function getCreators(stage?: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.creator.findMany({
    where: {
      userId: session.user.id,
      ...(stage && stage !== "all" ? { stage } : {}),
    },
    orderBy: { createdAt: "desc" },
  })
}

export async function updateCreator(
  creatorId: string,
  data: { stage?: string; notes?: string; score?: number; category?: string }
) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.creator.update({
    where: { id: creatorId, userId: session.user.id },
    data,
  })
}

export async function deleteCreator(creatorId: string) {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  await prisma.creator.delete({
    where: { id: creatorId, userId: session.user.id },
  })
  return { success: true }
}

export async function getSearchRuns() {
  const session = await auth()
  if (!session?.user?.id) throw new Error("Not authenticated")

  return prisma.searchRun.findMany({
    where: { userId: session.user.id },
    orderBy: { createdAt: "desc" },
    take: 20,
  })
}
