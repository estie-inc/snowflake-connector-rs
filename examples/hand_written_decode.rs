//! Hand-written [`FromCell`] and [`FromRow`] implementations, and a walkthrough of the decode error design.

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use url::Url;

use snowflake_connector_rs::{
    AuthConfig, CellPlan, CellPlanContext, Client, ClientConfig, ColumnType, EndpointConfig,
    ErrorKind, FromCell, FromRow, Result, RowPlanContext, RowRef, Session,
    decode::{
        CellConversionError, CellDecodeResult, CustomPlanError, PlanBuildResult,
        RowConversionError, RowDecodeResult,
    },
};

// A monetary amount in integer cents.
#[derive(Debug, PartialEq)]
struct Cents(i64);

// Per-column state for `Cents`: the decimal scale reported by the column, resolved once at plan time.
struct CentsPlan {
    scale: u8,
}

impl FromCell for Cents {
    type Plan = CentsPlan;

    fn build_plan(ctx: CellPlanContext<'_>) -> PlanBuildResult<Self::Plan> {
        // Plan time is where a hand-written decoder validates the column and precomputes reusable state. A NUMBER with
        // scale > 2 would silently lose precision when squeezed into cents, so reject it here rather than per row.
        match ctx.column().ty() {
            ColumnType::Fixed { scale, .. } => {
                let scale = scale.unwrap_or(0);
                if scale > 2 {
                    return Err(CustomPlanError::new(format!(
                        "Cents requires a NUMBER column with scale <= 2, found scale {scale}"
                    ))
                    .into());
                }
                Ok(CentsPlan { scale })
            }
            other => Err(CustomPlanError::new(format!(
                "Cents requires a fixed-point NUMBER column, found {}",
                other.as_str()
            ))
            .into()),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = raw.ok_or_else(|| CellConversionError::new("amount is NULL"))?;
        parse_cents(raw, plan.scale)
            .map(Cents)
            .map_err(CellConversionError::new)
    }
}

// Parse a decimal string such as "123.45" into integer cents, using the column scale to bound the fractional part.
fn parse_cents(raw: &str, scale: u8) -> std::result::Result<i64, String> {
    let (sign, digits) = match raw.strip_prefix('-') {
        Some(rest) => (-1_i64, rest),
        None => (1, raw),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));

    if whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("'{raw}' has a malformed integer part"));
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("'{raw}' has a malformed fractional part"));
    }
    if frac.len() > usize::from(scale) {
        return Err(format!(
            "'{raw}' has more fractional digits than the column scale {scale}"
        ));
    }

    let whole: i64 = whole
        .parse()
        .map_err(|_| format!("amount '{raw}' is out of range"))?;
    let frac_value: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse()
            .map_err(|_| format!("amount '{raw}' is out of range"))?
    };
    // `frac` is at most `scale` (<= 2) digits, so scaling it up to hundredths never overflows.
    let frac_cents = frac_value * 10_i64.pow(2 - frac.len() as u32);

    whole
        .checked_mul(100)
        .and_then(|cents| cents.checked_add(frac_cents))
        .map(|cents| sign * cents)
        .ok_or_else(|| format!("amount '{raw}' is out of range"))
}

// A transfer's lifecycle state, decoded from a short text code.
#[derive(Debug, PartialEq)]
enum Status {
    Active,
    Suspended,
    Closed,
}

impl FromCell for Status {
    // No per-column state is needed, so the plan is the unit type.
    type Plan = ();

    fn build_plan(ctx: CellPlanContext<'_>) -> PlanBuildResult<Self::Plan> {
        // Mirror the text-column check the built-in `String` decoder would apply, but report it as a custom validation.
        match ctx.column().ty() {
            ColumnType::Text { .. } => Ok(()),
            other => Err(CustomPlanError::new(format!(
                "Status requires a text column, found {}",
                other.as_str()
            ))
            .into()),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = raw.ok_or_else(|| CellConversionError::new("status is NULL"))?;
        // A code the domain does not recognize is a cell-local failure. The connector wraps this reason with the row
        // index, column, and raw value, surfacing it as a `CellDecodeError`.
        match raw {
            "ACTIVE" => Ok(Status::Active),
            "SUSPENDED" => Ok(Status::Suspended),
            "CLOSED" => Ok(Status::Closed),
            other => Err(CellConversionError::new(format!(
                "unknown status code '{other}'"
            ))),
        }
    }
}

// A whole row, composed from the cell decoders above.
#[derive(Debug, PartialEq)]
struct Transfer {
    source: String,
    destination: String,
    amount: Cents,
    status: Status,
}

// The resolved columns and their prepared cell plans, built once per result schema.
struct TransferPlan {
    source: CellPlan<String>,
    destination: CellPlan<String>,
    amount: CellPlan<Cents>,
    status: CellPlan<Status>,
}

impl FromRow for Transfer {
    type Plan = TransferPlan;

    fn build_plan(ctx: RowPlanContext<'_>) -> PlanBuildResult<Self::Plan> {
        // Hand-written code cannot construct `SchemaError` values, so a row-shape check reports a custom plan error.
        // No single column is in scope here, so this error carries no column context.
        let column_count = ctx.schema().len();
        if column_count != 4 {
            return Err(CustomPlanError::new(format!(
                "Transfer expects exactly 4 columns, found {column_count}"
            ))
            .into());
        }

        // `CellPlan::by_name` resolves each column and delegates to that cell type's `build_plan`, adding the column
        // context to any custom plan failure it raises.
        Ok(TransferPlan {
            source: CellPlan::by_name(ctx, "SOURCE")?,
            destination: CellPlan::by_name(ctx, "DESTINATION")?,
            amount: CellPlan::by_name(ctx, "AMOUNT")?,
            status: CellPlan::by_name(ctx, "STATUS")?,
        })
    }

    fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> RowDecodeResult<Self> {
        let source: String = row.get_with_plan(&plan.source)?;
        let destination: String = row.get_with_plan(&plan.destination)?;
        let amount = row.get_with_plan(&plan.amount)?;
        let status = row.get_with_plan(&plan.status)?;

        // A cross-cell domain rule: the individual cells decoded fine, but the combination is invalid.
        if source == destination {
            return Err(RowConversionError::new(format!(
                "a transfer cannot have the same source and destination ('{source}')"
            ))
            .into());
        }

        Ok(Transfer {
            source,
            destination,
            amount,
            status,
        })
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    run_happy_path().await?;
    run_plan_time_failure().await;
    run_cell_level_failure().await;
    run_row_level_failure().await;
    Ok(())
}

async fn run_happy_path() -> Result<()> {
    println!("== scenario: decode a well-formed result into Vec<Transfer> ==");
    let session = connect(HAPPY_PATH).await;

    let transfers = session
        .query_as("SELECT source, destination, amount, status FROM transfers")
        .await?
        .collect::<Vec<Transfer>>()
        .await?;

    assert_eq!(
        transfers,
        vec![
            Transfer {
                source: "alice".to_string(),
                destination: "bob".to_string(),
                amount: Cents(12345),
                status: Status::Active,
            },
            Transfer {
                source: "carol".to_string(),
                destination: "dave".to_string(),
                amount: Cents(99),
                status: Status::Suspended,
            },
            Transfer {
                source: "erin".to_string(),
                destination: "frank".to_string(),
                amount: Cents(100000),
                status: Status::Closed,
            },
        ]
    );
    println!("decoded {} transfers: {transfers:?}", transfers.len());
    Ok(())
}

async fn run_plan_time_failure() {
    println!("\n== scenario: plan-time failure (AMOUNT column is TEXT, not NUMBER) ==");
    let session = connect(AMOUNT_COLUMN_IS_TEXT).await;

    // `Cents::build_plan` rejects the column before any row is read, so the failure surfaces from `query_as`.
    let err = match session
        .query_as::<Transfer, _>("SELECT source, destination, amount, status FROM transfers")
        .await
    {
        Ok(_) => panic!("expected Cents::build_plan to reject a TEXT amount column"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), ErrorKind::Decode);
    let plan_err = err
        .as_custom_plan_error()
        .expect("plan-time failures expose a CustomPlanError");
    assert!(plan_err.reason().contains("fixed-point NUMBER"));
    // The reason comes from the implementation; the column context is filled in by the connector.
    assert_eq!(plan_err.column_name(), Some("AMOUNT"));
    assert_eq!(plan_err.column_index(), Some(2));

    println!(
        "reason   = {} (column {:?})",
        plan_err.reason(),
        plan_err.column_name()
    );
    println!("display  = {err}");
}

async fn run_cell_level_failure() {
    println!("\n== scenario: cell-level failure (unknown status code) ==");
    let session = connect(UNKNOWN_STATUS_CODE).await;

    // The schema is well-typed, so the plan builds; the failure happens while decoding a specific cell.
    let cursor = session
        .query_as("SELECT source, destination, amount, status FROM transfers")
        .await
        .expect("plan builds for a well-typed schema");
    let err = match cursor.collect::<Vec<Transfer>>().await {
        Ok(_) => panic!("expected an unknown status code to fail cell decoding"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), ErrorKind::Decode);
    let cell = err
        .as_cell_decode_error()
        .expect("cell conversion failures expose a CellDecodeError");
    assert_eq!(cell.row_index(), 1);
    assert_eq!(cell.column_name(), "STATUS");
    assert_eq!(cell.raw_value_preview(), Some("FROZEN"));
    assert_eq!(
        cell.conversion_error().reason(),
        "unknown status code 'FROZEN'"
    );

    println!(
        "row {} column {:?} value {:?}: {}",
        cell.row_index(),
        cell.column_name(),
        cell.raw_value_preview(),
        cell.conversion_error().reason()
    );
    println!("display  = {err}");
}

async fn run_row_level_failure() {
    println!("\n== scenario: row-level domain failure (source == destination) ==");
    let session = connect(SELF_TRANSFER).await;

    // Every cell decodes, but the second row violates a domain rule enforced in `from_row_with_plan`.
    let cursor = session
        .query_as("SELECT source, destination, amount, status FROM transfers")
        .await
        .expect("plan builds for a well-typed schema");
    let err = match cursor.collect::<Vec<Transfer>>().await {
        Ok(_) => panic!("expected a self-transfer to fail the row-level rule"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), ErrorKind::Decode);
    let row_err = err
        .as_row_conversion_error()
        .expect("row-level failures expose a RowConversionError");
    assert!(row_err.reason().contains("same source and destination"));
    // The connector fills in the failing row's index.
    assert_eq!(row_err.row_index(), Some(1));

    println!("row {:?}: {}", row_err.row_index(), row_err.reason());
    println!("display  = {err}");
}

/// Point a client at a mock server preloaded with one query response, then authenticate.
async fn connect(query_response: &'static str) -> Session {
    let base_url = spawn_mock_snowflake(vec![LOGIN_OK, query_response]);

    let config = ClientConfig::new(
        "demo-user",
        "demo-account",
        AuthConfig::password("demo-secret"),
    )
    .with_endpoint(EndpointConfig::custom_base_url(
        Url::parse(&base_url).expect("valid mock URL"),
    ));

    let client = Client::new(config).expect("client config is valid");
    client
        .create_session()
        .await
        .expect("mock accepts the login request")
}

/// Serve `responses` in order, one per incoming connection, then stop.
///
/// This is the same just-enough-HTTP mock used by `examples/error_handling.rs`.
fn spawn_mock_snowflake(responses: Vec<&'static str>) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
    let addr = listener.local_addr().expect("mock server address");

    thread::spawn(move || {
        for body in responses {
            let (mut stream, _) = listener.accept().expect("accept connection");
            // Read the request so the client can finish sending before we reply.
            let mut buf = [0_u8; 8192];
            let _ = stream.read(&mut buf);

            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        }
    });

    format!("http://{addr}")
}

const LOGIN_OK: &str = r#"{"success":true,"data":{"token":"session-token"}}"#;

// A well-formed result: three transfer rows, with AMOUNT as NUMBER(12, 2).
const HAPPY_PATH: &str = r#"{"success":true,"data":{"queryId":"01ab-happy","rowset":[["alice","bob","123.45","ACTIVE"],["carol","dave","0.99","SUSPENDED"],["erin","frank","1000.00","CLOSED"]],"rowtype":[{"name":"SOURCE","nullable":false,"length":16,"type":"text"},{"name":"DESTINATION","nullable":false,"length":16,"type":"text"},{"name":"AMOUNT","nullable":false,"precision":12,"scale":2,"type":"fixed"},{"name":"STATUS","nullable":false,"length":16,"type":"text"}],"queryResultFormat":"json"}}"#;

// AMOUNT is reported as TEXT, so `Cents::build_plan` rejects the column.
const AMOUNT_COLUMN_IS_TEXT: &str = r#"{"success":true,"data":{"queryId":"01ab-plan","rowset":[["alice","bob","123.45","ACTIVE"]],"rowtype":[{"name":"SOURCE","nullable":false,"length":16,"type":"text"},{"name":"DESTINATION","nullable":false,"length":16,"type":"text"},{"name":"AMOUNT","nullable":false,"length":16,"type":"text"},{"name":"STATUS","nullable":false,"length":16,"type":"text"}],"queryResultFormat":"json"}}"#;

// Row 1 carries a status code `Status` does not recognize.
const UNKNOWN_STATUS_CODE: &str = r#"{"success":true,"data":{"queryId":"01ab-cell","rowset":[["alice","bob","10.00","ACTIVE"],["carol","dave","5.50","FROZEN"]],"rowtype":[{"name":"SOURCE","nullable":false,"length":16,"type":"text"},{"name":"DESTINATION","nullable":false,"length":16,"type":"text"},{"name":"AMOUNT","nullable":false,"precision":12,"scale":2,"type":"fixed"},{"name":"STATUS","nullable":false,"length":16,"type":"text"}],"queryResultFormat":"json"}}"#;

// Row 1 transfers to itself, violating the row-level domain rule.
const SELF_TRANSFER: &str = r#"{"success":true,"data":{"queryId":"01ab-row","rowset":[["alice","bob","10.00","ACTIVE"],["carol","carol","5.50","ACTIVE"],["erin","frank","1.00","CLOSED"]],"rowtype":[{"name":"SOURCE","nullable":false,"length":16,"type":"text"},{"name":"DESTINATION","nullable":false,"length":16,"type":"text"},{"name":"AMOUNT","nullable":false,"precision":12,"scale":2,"type":"fixed"},{"name":"STATUS","nullable":false,"length":16,"type":"text"}],"queryResultFormat":"json"}}"#;
