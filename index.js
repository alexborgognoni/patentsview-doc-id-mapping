import fs from "fs";
import got from "got";
import { createWriteStream } from "fs";
import { promisify } from "util";
import { pipeline } from "stream";
import { once } from "events";

const pipelineAsync = promisify(pipeline);

async function main() {
  // Read patent IDs from file
  const patentIdsPath = "./patent_ids.txt";
  const patentIds = fs.readFileSync(patentIdsPath, "utf-8").split("\n");

  // Define the output file path
  const mappingFilePath = "./patent_id_assignee_mapping.csv";

  // Create a writable stream for the output file
  const outStream = createWriteStream(mappingFilePath);
  outStream.write("patent_id,assignee_id,assignee_organization\n");

  // Initialize progress variables
  const totalPatents = patentIds.length;
  let processedPatents = 0;

  // Loop through patentIds
  for (const patentId of patentIds) {
    const url = `https://api.patentsview.org/assignees/query?q={"_eq":{"patent_id":"${patentId}"}}`;

    try {
      const response = await got(url, { responseType: "json" });
      const data = response.body;
      const assignees = data.assignees || [];

      // Write assignee data to the output file
      for (const assignee of assignees) {
        const { assignee_id, assignee_organization } = assignee;
        const organization = assignee_organization
          ? assignee_organization.replace(/,/g, ";")
          : "";
        outStream.write(`${patentId},${assignee_id},${organization}\n`);
      }
    } catch (error) {
      console.error(
        `Error fetching data for patent ID ${patentId}:`,
        error.message,
      );
    }

    // Update progress
    processedPatents++;
    const progressPercentage = (processedPatents / totalPatents) * 100;
    console.log(`Progress: ${progressPercentage.toFixed(2)}%`);
  }

  // Close the output file stream
  outStream.end();
  console.log("Finished processing all patent IDs.");
}

// Execute the main function
main().catch((error) => console.error("Error:", error));
