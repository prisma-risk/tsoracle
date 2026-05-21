//! Macros for declaring an openraft `RaftTypeConfig` with sensible defaults.
//!
//! Consumers supply only the slots that actually vary (`Node`, `AppData`,
//! `AppDataResponse`, `SnapshotData`, optionally `NodeId`/`Entry`/`AsyncRuntime`/
//! `Responder`); everything else is inherited from the defaults below.
//!
//! # Why a direct trait impl instead of wrapping `openraft::declare_raft_types!`
//!
//! openraft 0.10.0-alpha.20's `declare_raft_types!` macro does not accept a
//! `Responder<T> = ...` override — its `Responder<T>` slot has a `where` clause
//! that the macro's `$type_id:ident = $type:ty` entry grammar can't express.
//! The openraft test file (`raft/declare_raft_types_test.rs`) calls this out
//! explicitly:
//!
//! ```text
//! // Responder<T> is not supported by  declare_raft_types
//! ```
//!
//! The openraft macro's hard-coded default is `ProgressResponder<Self, T>`.
//! Defaulting to `OneshotResponder<Self, T>` instead therefore requires
//! emitting an `impl RaftTypeConfig` block directly. That also gives the macro
//! full control over `LeaderId`, `Vote`, `Entry`, `Batch`, and `ErrorSource`,
//! so the produced impl is identical to a hand-written one.

/// Declare a `RaftTypeConfig` with sensible defaults.
///
/// Required slots:
/// - `Node` — peer metadata (must satisfy openraft's `Node` bounds:
///   `Clone + Default + Eq + PartialEq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static`)
/// - `AppData` — log entry payload (openraft's `D`)
/// - `AppDataResponse` — state-machine apply result (openraft's `R`)
/// - `SnapshotData` — snapshot wire format (must satisfy openraft's
///   `AsyncRead + AsyncSeek + AsyncWrite + Unpin + Send + 'static`)
///
/// Optional overrides:
/// - `NodeId` (default `u64`)
/// - `Entry` (default
///   `openraft::Entry<<Self::LeaderId as openraft::vote::RaftLeaderId>::Committed, Self::D, Self::NodeId, Self::Node>`)
/// - `AsyncRuntime` (default `openraft::impls::TokioRuntime`)
/// - `Responder` — accepts a **fully instantiated** `Responder<C, T>` type
///   where `C` and `T` are the GAT's in-scope `Self` and `T` (i.e. write the
///   override as `openraft::impls::OneshotResponder<Self, T>`). Default
///   `openraft::impls::OneshotResponder<Self, T>`.
///
/// Inherited (non-overridable) defaults:
/// - `Term = u64`
/// - `LeaderId = openraft::impls::leader_id_adv::LeaderId<Self::Term, Self::NodeId>`
///   (multi-leader-per-term)
/// - `Vote = openraft::impls::Vote<Self::LeaderId>`
/// - `Batch<T> = openraft::impls::InlineBatch<T>`
/// - `ErrorSource = openraft::impls::BoxedErrorSource`
///
/// # Examples
///
/// ```ignore
/// use tsoracle_openraft_toolkit::declare_raft_types_ext;
///
/// declare_raft_types_ext! {
///     pub MyTypeConfig:
///         Node            = MyPeer,
///         AppData         = MyLogEntry,
///         AppDataResponse = MyAppliedState,
///         SnapshotData    = std::io::Cursor<Vec<u8>>,
/// }
/// ```
#[macro_export]
macro_rules! declare_raft_types_ext {
    (
        $vis:vis $name:ident :
        $( NodeId          = $node_id:ty , )?
        Node               = $node:ty ,
        $( Entry           = $entry:ty , )?
        AppData            = $app_data:ty ,
        AppDataResponse    = $app_resp:ty ,
        SnapshotData       = $snap:ty ,
        $( AsyncRuntime    = $runtime:ty , )?
        $( Responder       = $responder:ty , )?
    ) => {
        #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
        $vis struct $name {}

        impl ::openraft::RaftTypeConfig for $name {
            type D            = $app_data;
            type R            = $app_resp;
            type NodeId       = $crate::__first_or_default_node_id!($( $node_id, )?);
            type Node         = $node;
            type Term         = u64;
            type LeaderId     = ::openraft::impls::leader_id_adv::LeaderId<Self::Term, Self::NodeId>;
            type Vote         = ::openraft::impls::Vote<Self::LeaderId>;
            type Entry        = $crate::__first_or_default_entry!($( $entry, )?);
            type SnapshotData = $snap;
            type AsyncRuntime = $crate::__first_or_default_runtime!($( $runtime, )?);
            type Responder<T>
                = $crate::__first_or_default_responder!($( $responder, )?)
            where
                T: ::openraft::OptionalSend + 'static;
            type Batch<T>
                = ::openraft::impls::InlineBatch<T>
            where
                T: ::openraft::OptionalSend + 'static;
            type ErrorSource  = ::openraft::impls::BoxedErrorSource;
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __first_or_default_node_id {
    ($t:ty,) => {
        $t
    };
    () => {
        u64
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __first_or_default_entry {
    ($t:ty,) => { $t };
    ()       => {
        ::openraft::Entry<
            <Self::LeaderId as ::openraft::vote::RaftLeaderId>::Committed,
            Self::D,
            Self::NodeId,
            Self::Node,
        >
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __first_or_default_runtime {
    ($t:ty,) => {
        $t
    };
    () => {
        ::openraft::impls::TokioRuntime
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __first_or_default_responder {
    // Caller supplies the fully instantiated `Responder<C, T>` type using the
    // GAT's in-scope `Self` and `T` (e.g. `OneshotResponder<Self, T>`).
    ($t:ty,) => { $t };
    // No override; default to OneshotResponder<Self, T>. `Self` and `T` here
    // refer to the impl block's `Self` and the GAT's `T`, by macro hygiene
    // through the expansion site.
    ()       => { ::openraft::impls::OneshotResponder<Self, T> };
}
