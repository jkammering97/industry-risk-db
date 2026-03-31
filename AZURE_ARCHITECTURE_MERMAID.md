# Azure Architecture Mermaid

This Mermaid visual uses the local Azure SVG icons in `azure_icons/` and groups the solution into:

- Website
- Networking
- Azure Cloud
- Local Workspace

If your Mermaid renderer supports `img:` nodes, this version will display the actual Azure icons.

```mermaid
flowchart LR
    %% Website layer
    subgraph website["Website Experience"]
        user["Supply Chain Manager\nor Procurement Lead"]
        linkedin["LinkedIn / shared links"]
        browser["Browser session\nlanding page + observer"]
    end

    %% Networking layer
    subgraph networking["Networking and Access"]
        publicUrl["Azure Container Apps URL\ncountry-risk-container...azurecontainerapps.io"]
        customDomain["Optional custom domain\nrisk.yourdomain.com"]
        dns@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/networking/10064-icon-service-DNS-Zones.svg", label: "DNS Zone", pos: "b", w: 64, h: 64, constraint: "on" }
        publicIp@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/networking/10069-icon-service-Public-IP-Addresses.svg", label: "Public endpoint", pos: "b", w: 64, h: 64, constraint: "on" }
    end

    %% Azure cloud layer
    subgraph cloud["Azure Cloud Runtime"]
        ca@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/other/02884-icon-service-Worker-Container-App.svg", label: "Container App\ncountry-risk-container", pos: "b", w: 72, h: 72, constraint: "on" }
        env@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/other/02989-icon-service-Container-Apps-Environments.svg", label: "Container Apps Environment\nmanagedEnvironment-RiskObserverRG", pos: "b", w: 72, h: 72, constraint: "on" }
        store@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/storage/10086-icon-service-Storage-Accounts.svg", label: "Storage Account + Tables\ncountrytoriskstorage", pos: "b", w: 72, h: 72, constraint: "on" }
        logs@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/monitor/00009-icon-service-Log-Analytics-Workspaces.svg", label: "Log Analytics Workspace", pos: "b", w: 72, h: 72, constraint: "on" }
        kv@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/security/10245-icon-service-Key-Vaults.svg", label: "Key Vault\nlegacy / optional", pos: "b", w: 72, h: 72, constraint: "on" }
        mi@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/identity/10227-icon-service-Managed-Identities.svg", label: "Managed Identity\noptional", pos: "b", w: 72, h: 72, constraint: "on" }
        sql@{ img: "/Users/joey/Desktop/work/industy_risk_db/azure_icons/databases/10130-icon-service-SQL-Database.svg", label: "Azure SQL Database\nlegacy rich mart source", pos: "b", w: 72, h: 72, constraint: "on" }
        observer["Streamlit app\nrisk_dashboard_sql.py"]
        landing["Landing page\nproduct story + value framing"]
        charts["Observer UI\nKPIs, bar chart, sunburst, tabs"]
        layers["Table-backed layer data\nHHI, logistics, policy"]
    end

    %% Local workspace layer
    subgraph local["Local Workspace and Data Jobs"]
        repo["Local repo\nindustry_risk_db"]
        docker["Docker buildx image\nkammer97/country-risk-container:*"]
        migration["migrate_sql_marts_to_tables.py\nSQL -> Azure Tables"]
        loader["load_risk_layers_to_tables.py\nComtrade -> Azure Tables"]
        comtrade["UN Comtrade API"]
    end

    %% Website flow
    user --> browser
    linkedin -. shared link .-> browser
    browser --> customDomain
    browser --> publicUrl

    %% Networking flow
    customDomain -. optional DNS mapping .-> dns
    dns --> publicIp
    publicUrl --> publicIp
    publicIp --> ca

    %% Runtime flow
    ca --> observer
    observer --> landing
    observer --> charts
    charts --> layers
    layers --> store

    %% Azure dependencies
    ca --> env
    env --> logs
    ca -. optional secret access .-> kv
    ca -. optional workload identity .-> mi

    %% Local delivery and ingestion
    repo --> docker
    docker -. publish image .-> ca
    sql --> migration
    migration --> store
    comtrade --> loader
    loader --> store

    %% Styling
    classDef websiteNode fill:#f6efe3,stroke:#8c6b3f,color:#2f2214,stroke-width:1.4px;
    classDef networkNode fill:#eaf2fb,stroke:#4b76b5,color:#18304e,stroke-width:1.4px;
    classDef cloudNode fill:#eef7f4,stroke:#2f6b5d,color:#12303b,stroke-width:1.4px;
    classDef localNode fill:#f5effa,stroke:#7c5ea7,color:#36224f,stroke-width:1.4px;

    class user,linkedin,browser websiteNode;
    class publicUrl,customDomain networkNode;
    class observer,landing,charts,layers cloudNode;
    class repo,docker,migration,loader,comtrade localNode;
```

## Notes

- `customDomain` is marked optional because the current deployment is still using the Azure Container Apps URL.
- `Azure SQL Database` is shown as a legacy rich source because you are migrating the detailed mart data into Azure Tables.
- `Key Vault` and `Managed Identity` are shown as optional/legacy because they may be removed as part of the cost cleanup.
