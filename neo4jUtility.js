// TODO: This file is same as the file created in the api-gateway (Reelz_server) so maybe I should make a git submodule
// or an npm package so that I dont write this code again
import neo4j from 'neo4j-driver';

export const driver = neo4j.driver(process.env.NEO4J_URI, neo4j.auth.basic(process.env.NEO4J_USERNAME, process.env.NEO4J_PASSWORD));

/**
 * Execute a raw Cypher query with parameters. User this function for complex queries
 * @param {string} queryStr - Raw Cypher Query
 * @param {object} params - Query parameters
 * @param {string} name - Name of the query
 * @returns {Promise<object[]>} Query result
 */
export async function neo4jQuery(queryStr, params = {}, name="default") {
  const session = driver.session();
  try {
    const result = await session.run(queryStr, params);
    return result.records;
  } catch(err) {
    console.error("Neo4j Error on the query " + name + ":", err);
  }
}