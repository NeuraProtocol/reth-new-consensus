fn main() {
    // Define the Narwhal consensus service
    let consensus_service = anemo_build::manual::Service::builder()
        .name("NarwhalConsensus")
        .package("narwhal")
        .method(
            anemo_build::manual::Method::builder()
                .name("submit_header")
                .route_name("SubmitHeader")
                .request_type("crate::types::Header")
                .response_type("crate::rpc::HeaderResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("submit_vote")
                .route_name("SubmitVote")
                .request_type("crate::types::Vote")
                .response_type("crate::rpc::VoteResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("submit_certificate")
                .route_name("SubmitCertificate")
                .request_type("crate::types::Certificate")
                .response_type("crate::rpc::CertificateResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("get_certificates")
                .route_name("GetCertificates")
                .request_type("crate::rpc::GetCertificatesRequest")
                .response_type("crate::rpc::GetCertificatesResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .build();

    // Define the DAG service for batch and transaction handling  
    let dag_service = anemo_build::manual::Service::builder()
        .name("NarwhalDag")
        .package("narwhal.dag")
        .method(
            anemo_build::manual::Method::builder()
                .name("submit_batch")
                .route_name("SubmitBatch")
                .request_type("crate::Batch")
                .response_type("crate::rpc::BatchResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("get_batch")
                .route_name("GetBatch")
                .request_type("crate::rpc::GetBatchRequest")
                .response_type("crate::rpc::GetBatchResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .build();

    // Define the Worker-to-Worker service for batch replication
    let worker_service = anemo_build::manual::Service::builder()
        .name("WorkerToWorker")
        .package("narwhal.worker")
        .method(
            anemo_build::manual::Method::builder()
                .name("send_message")
                .route_name("SendMessage")
                .request_type("crate::rpc::WorkerMessage")
                .response_type("()")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("request_batches")
                .route_name("RequestBatches")
                .request_type("crate::rpc::WorkerBatchRequest")
                .response_type("crate::rpc::WorkerBatchResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .build();
    
    // Define the Primary-to-Worker service
    let primary_worker_service = anemo_build::manual::Service::builder()
        .name("PrimaryToWorker")
        .package("narwhal.primary")
        .method(
            anemo_build::manual::Method::builder()
                .name("send_message")
                .route_name("SendMessage")
                .request_type("crate::rpc::PrimaryWorkerMessage")
                .response_type("()")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("synchronize")
                .route_name("Synchronize")
                .request_type("crate::rpc::WorkerSynchronizeMessage")
                .response_type("()")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("request_batch")
                .route_name("RequestBatch")
                .request_type("crate::rpc::RequestBatchRequest")
                .response_type("crate::rpc::RequestBatchResponse")
                .codec_path("anemo::rpc::codec::BincodeCodec")
                .build(),
        )
        .build();

    anemo_build::manual::Builder::new()
        .compile(&[consensus_service, dag_service, worker_service, primary_worker_service]);
} 