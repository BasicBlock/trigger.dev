import { python } from "@basicblock/trigger-python";
import { ai } from "@basicblock/trigger-sdk/ai";
import { schemaTask } from "@basicblock/trigger-sdk";
import { z } from "zod";

const crawlerTask = schemaTask({
  id: "crawler",
  description: "Crawl a URL and return the markdown",
  schema: z.object({
    url: z.string().describe("The URL to crawl"),
  }),
  run: async ({ url }) => {
    const results = await python.runScript("./src/trigger/python/crawler.py", [url]);

    return results.stdout;
  },
});

export const crawler = ai.tool(crawlerTask);
