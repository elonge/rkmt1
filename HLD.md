graph TD
    subgraph "Offline / Batch Processing (Daily)"
        Raw[Raw WhatsApp Data (Mongo)] --> ETL[ETL Pipeline]
        ETL --> Profiler[User Profiling Engine]
        ETL --> Narrative[Narrative Clusterer]
        
        Profiler --> UserDB[(Structured User DB)]
        Narrative --> VectorDB[(Vector Store)]
        Narrative --> KG[(Knowledge Graph / Summaries)]
    end

    subgraph "Online / Real-time Agent"
        User[User Query] --> Planner[LLM Planner / Router]
        
        Planner --> ToolStats[Tool: Structured Query (SQL/Mongo)]
        Planner --> ToolRAG[Tool: Narrative RAG]
        Planner --> ToolAudience[Tool: Audience Builder]
        
        ToolStats <--> UserDB
        ToolAudience <--> UserDB
        ToolRAG <--> VectorDB & KG
        
        ToolStats & ToolRAG & ToolAudience --> Synthesizer[LLM Synthesizer]
        Synthesizer --> Final[Final Response]
    end