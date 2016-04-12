# KE4IR Evaluation Code

[KE4IR](http://pikes.fbk.eu/ke4ir) is the prototype implementation (for research evaluation) of an Information Retrieval approach where documents and queries are processed with a Knowledge Extraction tool ([PIKES](http://pikes.fbk.eu/) to extract structured knowledge from them, which is then used to augment their term vector representations with *semantic terms*.
An ESWC 2016 paper describing KE4IR can be found [here](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2016eswc.pdf) (preprint).

This GitHub project contains the Java code (based on [Lucene](http://lucene.apache.org/c), [Sesame](http://rdf4j.org/), and [RDFpro](http://rdfpro.fbk.eu/)) implementing a simple evaluation system that allow configuring and evaluating KE4IR on arbitrary document collections and queries for which relevance judgments are known. You can use this code, together with the data available on KE4IR [webpage](http://pikes.fbk.eu/ke4ir), to replicate the evaluation results reported in the KE4IR paper. You can also use this code as a basis for experimenting with a variation of KE4IR, or even with a different approach that can be casted in the framework of KE4IR (augmentation of term vectors with semantic terms obtained via knowledge extraction).

## Installation

This code is released open source using Maven. To install it, just type into the shell:

  * `git clone https://github.com/dkmfbk/ke4ir-evaluation.git`
  * `cd ke4ir-evaluation`
  * `git checkout -b develop origin/develop` (to use the latest code from the `develop` branch)
  * `mvn clean package -DskipTests -Prelease`
  * `cd target`
  * `tar xf ke4ir-0.1-SNAPSHOT-bin.tar.gz`
  * `cd ke4ir`

Now you can start the tool by simply using the `./ke4ir-eval` script. If your want, you can move the `ke4ir` folder containing the compiled binaries in any path of your choice.

## Usage

The tool `ke4ir-eval` requires:
 
  * the NLP annotations of documents and queries, in the [NAF](http://wordpress.let.vupr.nl/naf/) format (as produced by PIKES);
  * the RDF knowledge graphs extracted by PIKES for both documents and queries;
  * an index (possibly empty) containing background knowledge to be injected in the knowledge graphs; 
  * gold relevance judgments of queries
 * a configuration file (a .properties file) that specifies where to find all the above data, plus which components and settings use for the different steps performed by KE4IR.

You can find a ZIP file with all the above data for the evaluation reported in the ESWC paper on KE4IR webpage ([direct link](https://knowledgestore.fbk.eu/files/ke4ir/ke4ir_evaluation.zip)).

Running `ke4ir-eval` requires specifying a configuration file and the desired actions to perform (e.g., index documents and queries, evaluate queries). The configuration file is specified with option `-p`; if omitted, the tool looks for a file `ke4ir.properties` in the current directory. Here is the full list of options:

```
usage: ke4ir-eval [-a] [--analyze-docs] [--analyze-queries] [-c <PATH>] [-e]
       [--enrich-docs] [--enrich-queries] [-h] [-i] [-p <PATH>] [-s] [-v]

supports all the operations involved in the evaluation of semantic information
retrieval: enrichment, analysis, indexing, search

  -a,--analyze               analyzes both documents and queries (NAF + RDF
                             enriched)
     --analyze-docs          analyzes documents only (NAF + RDF enriched)
     --analyze-queries       analyzes queries only (NAF + RDF enriched)
  -e,--enrich                enriches the RDF of both documents and queries
     --enrich-docs           enriches the RDF of documents only
     --enrich-queries        enriches the RDF of queries only
  -h,--help                  display this help message and terminate
  -i,--index                 indexes document terms in Lucene
  -p,--properties <PATH>     specifies the configuration properties file
  -s,--search                evaluates queries over Lucene index
  -v,--version               display version information and terminate
```

Options `-a`, `-e`, `-i`, and `-s` trigger the following actions (they can be controlled also via `ke4ir.properties`):

  * *enrichment* (`-e`), consisting in augmenting the RDF knowledge graphs of each document and query with background knowledge coming from a key-value index (e.g., type triples from DBpedia);
  * *analysis* (`-a`), consisting in the extraction of textual and semantic terms from NAF files and enriched RDF data of documents and queries;
  * *indexing* (`-i`), consisting in the construction of a Lucene index with (textual, semantic) terms of documents, obtained in the analysis step;
  * *search* (`-s`), consisting in the evaluation of queries over the previously built index, and the comparison of the obtained rankings with gold relevance judgments, which results in the computation of a number of evaluation metrics.

When using `ke4ir-eval` on the data distributed on KE4IR webpage, enrichment was already performed and only analysis, indexing, and search has to be performed to replicate our evaluation results (the distributed `ke4ir.properties` configuration file is already setup to perform the execution of these actions).
