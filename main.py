import asyncio
import aiohttp
import csv
import multiprocessing


async def fetch_assignees(session, patent_id, csv_writer, semaphore):
    url = f"https://api.patentsview.org/assignees/query?q={{\"_eq\":{{\"patent_id\":\"{patent_id}\"}}}}"
    async with semaphore:
        async with session.get(url) as response:
            if response.status == 200:
                assignees_data = await response.json()
                assignees = assignees_data.get('assignees', [])
                if (assignees):
                    for assignee in assignees:
                        if (assignee):
                            assignee_id = assignee.get('assignee_id', '')
                            assignee_organization = assignee.get(
                                'assignee_organization', '')
                            if (assignee_organization):
                                assignee_organization = assignee_organization.replace(
                                    ',', ';')
                            csv_writer.writerow(
                                [patent_id, assignee_id, assignee_organization])
                else:
                    csv_writer.writerow(
                        [patent_id, None, None])
            else:
                print(
                    f"Error fetching patent_id {patent_id}: {response.status}")


async def process_patent_ids(patent_ids_batch, csv_writer, semaphore, session):
    for patent_id in patent_ids_batch:
        await fetch_assignees(session, patent_id, csv_writer, semaphore)


async def main():
    patent_ids_path = "./patent_ids.txt"
    with open(patent_ids_path, "r") as f:
        patent_ids = f.read().splitlines()

    mapping_file_path = "./patent_id_assignee_mapping.csv"
    # Write the header if the file doesn't exist
    header_written = False
    with open(mapping_file_path, "a", newline='') as f:
        csv_writer = csv.writer(f)
        for patent_id in patent_ids:
            if not header_written:
                csv_writer.writerow(
                    ["patent_id", "assignee_id", "assignee_organization"])
                header_written = True

    # Multiprocessing setup
    num_processes = multiprocessing.cpu_count()  # Number of CPU cores

    semaphore = asyncio.Semaphore(40)  # Adjust as per the rate limit

    # Split patent IDs into batches
    batch_size = len(patent_ids) // num_processes
    patent_id_batches = [patent_ids[i:i+batch_size]
                         for i in range(0, len(patent_ids), batch_size)]

    # Start processes for each batch of patent IDs
    processes = []
    async with aiohttp.ClientSession() as session:
        with open(mapping_file_path, "a", newline='') as f:
            csv_writer = csv.writer(f)
            for patent_id_batch in patent_id_batches:
                process = multiprocessing.Process(target=asyncio.run, args=(await process_patent_ids(
                    patent_id_batch, csv_writer, semaphore, session),))
                processes.append(process)
                process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()

    print("Finished processing all patent IDs.")


if __name__ == "__main__":
    asyncio.run(main())
