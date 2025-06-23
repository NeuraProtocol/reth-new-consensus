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

    anemo_build::manual::Builder::new()
        .compile(&[consensus_service, dag_service]);
} 