export type Talk = {
  conf: string;
  date: string;
  image: string;
  title: string;
  description: string;
  video_url: string;
  conf_url: string;
  speakers: string[];
};

export const Talks: Talk[] = [
  {
    conf: "move(data) 2025",
    date: "2025-03-20",
    image: "move_data_2025.png",
    title: "Following the Data Breadcrumbs: Data Lineage using OpenLineage",
    speakers: ["Rahul Madan, Atlan"],
    description:
      "Whether you're troubleshooting data issues, planning pipeline changes, or ensuring compliance, this session will equip you with the knowledge to leverage OpenLineage for better data visibility and control. Demystify data lineage and its critical role in modern data ecosystems, explore real-world use cases that demonstrate the power of lineage tracking, and learn how following your data's breadcrumbs (Data Lineage) can transform your team's efficiency and data reliability!",
    video_url: "https://www.youtube.com/watch?v=fSL110nYCYo&list=PLgyvStszwUHj-HwMVPGQAWRwhnZSkePRA",
    conf_url: "https://movedata.airbyte.com/",
  },
  {
    conf: "Airflow Summit 2024",
    date: "2024-09-10",
    image: "airflow_summit_v2.png",
    title: "Activating operational metadata with Airflow, Atlan and OpenLineage",
    speakers: ["Kacper Muda, GetInData"],
    description:
      "We will demonstrate what OpenLineage is and how, with minimal and intuitive setup across Airflow and Atlan, it presents unified workflows view, efficient cross-platform lineage collection, including column level, in various technologies (Python, Spark, dbt, SQL etc.) and clouds (AWS, Azure, GCP, etc.) - all orchestrated by Airflow.",
    video_url: "https://www.youtube.com/embed/z_j3UFIyj9o?si=KTmK1C-N9LlBhpRg",
    conf_url:
      "https://airflowsummit.org/sessions/2024/activating-operational-metadata-with-airflow-atlan-and-openlineage/",
  },
  {
    conf: "Kafka Summit London 2024",
    date: "2024-03-22",
    image: "kafka_summit_v2.png",
    title: "OpenLineage for Stream Processing",
    speakers: ["Maciej Obuchowski, GetInData", "Paweł Leszczyński, Astronomer"],
    description:
      "This talk will provide an overview of the most recent developments in the OpenLineage Flink integration and share what’s in store for this important collaboration.",
    video_url: "https://www.confluent.io/events/kafka-summit-london-2024/openlineage-for-stream-processing/",
    conf_url: "https://www.confluent.io/events/kafka-summit-london-2024/openlineage-for-stream-processing/",
  },
  {
    conf: "Data Council Austin 2024",
    date: "2024-03-26",
    image: "data_council.png",
    title: "Data Lineage: We've Come a Long Way",
    speakers: [
      "Harel Shein, Datadog",
      "Ernie Ostic, Manta Software",
      "Sheeri Cabral, Collibra",
      "Eric Veleker, Atlan",
      "Moderator: Julien Le Dem, OpenLineage Project Lead",
    ],
    description:
      "This panel discussion features industry experts from the Data Catalog and Data Observability space as they explore the evolution and future prospects of data lineage. Discover how technology has progressed, from manual approaches to automated operational lineage across batch and stream processing, and grasp the pivotal role of data lineage in data processing layers.",
    video_url: "https://www.youtube.com/watch?v=OE1o4D_iWfw/",
    conf_url: "https://www.datacouncil.ai/talks24/panel-data-lineage-weve-come-a-long-way/",
  },
  {
    conf: "Data+AI Summit 2024",
    date: "2024-06-10",
    image: "data+ai_summit.png",
    title: "Cross-Platform Data Lineage with OpenLineage",
    speakers: ["Julien Le Dem, Chief Architect", "Willy Lulciuc, Astronomer"],
    description:
      "OpenLineage provides a standard for lineage collection that spans multiple platforms, including Apache Airflow®, Apache Spark™, Flink®, and dbt. This empowers teams to diagnose and address widespread data quality and efficiency issues in real time. In this session, we will show how to trace data lineage across Apache Spark and Apache Airflow®. There will be a walk-through of the OpenLineage architecture and a live demo of a running pipeline with real-time data lineage.",
    video_url: "https://www.youtube.com/watch?v=rO3BPqUtWrI/",
    conf_url: "https://moscone.com/events/data-ai-summit-2024/",
  },
  {
    conf: "Berlin Buzzwords 2023",
    date: "2023-06-18",
    image: "buzzwords_2023.png",
    title: "Column-level Lineage is Coming to the Rescue",
    speakers: ["Paweł Leszczyński, OpenLineage", "Maciej Obuchowski, GetInData"],
    description:
      "OpenLineage is a standard for metadata and lineage collection that is growing rapidly. Column-level lineage is one of its most anticipated features of the community that has been developed recently. In this talk, we show foundations for column lineage within OpenLineage standard, provide real-life demo on how is it automatically extracted from Spark jobs, describe and demo column lineage extraction from SQL queries, show how the lineage can be consumed on Marquez backend. We aim to provide demos to focus on practical aspects of the column-level lineage which are interesting to data practitioners all over the world.",
    video_url: "https://www.youtube.com/watch?v=xFVSZCCbZlY/",
    conf_url: "https://2023.berlinbuzzwords.de/sessions/?id=NPKZHP/",
  },
  {
    conf: "Berlin Buzzwords 2022",
    date: "2023-06-13",
    image: "buzzwords_2022.png",
    title: "Cross-platform Data Lineage with OpenLineage",
    speakers: ["Julien Le Dem, OpenLineage"],
    description:
      "In this session, Julien Le Dem will show how to trace data lineage across Apache Spark and Apache Airflow. He will walk through the OpenLineage architecture and provide a live demo of a running pipeline with real-time data lineage.",
    video_url: "https://www.youtube.com/watch?v=pLBVGIPuwEo/",
    conf_url: "https://pretalx.com/bbuzz22/talk/FHEHAL/",
  },
  {
    conf: "Berlin Buzzwords 2021",
    date: "2023-06-13",
    image: "buzzwords_2021.png",
    title: "Observability for Data Pipelines with OpenLineage",
    speakers: ["Julien Le Dem, OpenLineage"],
    description:
      "Data is increasingly becoming core to many products. Whether to provide recommendations for users, getting insights on how they use the product or using machine learning to improve the experience. This creates a critical need for reliable data operations and understanding how data is flowing through our systems. Data pipelines must be auditable, reliable and run on time. This proves particularly difficult in a constantly changing, fast-paced environment.",
    video_url: "https://www.youtube.com/watch?v=HEJFCQLwdtk/",
    conf_url: "https://2025.berlinbuzzwords.de/",
  },
  {
    conf: "Data Driven NYC",
    date: "2021-02-01",
    image: "data_driven_nyc.png",
    title: "Data Observability and Pipelines: OpenLineage and Marquez",
    speakers: ["Julien Le Dem, OpenLineage"],
    description:
      "Julien Le Dem presented at Data Driven NYC in January 2021. He spoke about the need for an end-to-end management layer for data, how this strengthens the overall data ops landscape, and the launch of OpenLineage, a new effort to define such a flexible industry standard for data lineage.",
    video_url: "https://www.youtube.com/watch?v=MoW-YGjHLgI/",
    conf_url: "https://mattturck.com/datakin/",
  },
  {
    conf: "Open Core Summit 2020",
    date: "2020-12-16",
    image: "ocs_2020.png",
    title: "OCS Breakout: Julien Le Dem",
    speakers: ["Julien Le Dem, OpenLineage"],
    description:
      "Julien Le Dem co-created Apache Parquet and is involved in several open source projects including Marquez (LF AI), Apache Pig, Apache Arrow, Apache Iceberg and a few others. Previously, Julien was a senior principal at Wework; principal architect at Dremio; tech lead for Twitter’s data processing tools, where he also obtained a two-character Twitter handle (@J_); and a principal engineer and tech lead working on content platforms at Yahoo, where he received his Hadoop initiation. His French accent makes his talks particularly attractive.",
    video_url: "https://www.youtube.com/watch?v=MoW-YGjHLgI/",
    conf_url: "https://www.coss.community/cossc/ocs-2020-breakout-julien-le-dem-3eh4/",
  },
];
