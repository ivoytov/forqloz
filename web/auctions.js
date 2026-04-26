const minTransactionPrice = 10000
let markers = {};
let mapRenderSeq = 0;
const mapFeatureCache = new Map();


// Use SQLite DB (sql.js) only — no CSV fallback
const DB_PATH = 'foreclosures/foreclosures.sqlite'
let combinedData = []

async function loadDB() {
    try {
        if (typeof initSqlJs !== 'function') return null;
        // Load wasm assets from CDN
        const SQL = await initSqlJs({ locateFile: f => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${f}` });
        const resp = await fetch(DB_PATH);
        if (!resp.ok) return null;
        const buf = await resp.arrayBuffer();
        const db = new SQL.Database(new Uint8Array(buf));

        const run = (sql) => {
            const res = db.exec(sql);
            if (!res || res.length === 0) return [];
            const { columns, values } = res[0];
            return values.map(row => Object.fromEntries(row.map((v, i) => [columns[i], v])));
        };

        const sales = run('SELECT * FROM auction_sales');
        const lots = run(`
            SELECT
                lots.case_number,
                lots.borough,
				boroughs.id as borough_id,
				boroughs.code as borough_code,
                lots.block,
                lots.lot,
                lots.address AS lot_address,
                lots.BBL,
                lots.unit,
                cases.auction_date,
                cases.case_name,
                bids.judgement,
                bids.upset_price,
                bids.winning_bid,
                CASE WHEN bids.winning_bid > 100 THEN bids.winning_bid - bids.upset_price END AS over_bid,
                pluto.Address AS pluto_address,
                pluto.ZipCode,
                COALESCE(substr(building_class.name, 0, instr(building_class.name, ':')), pluto.BldgClass) as LandUse,
                COALESCE(building_class.name, pluto.BldgClass) as BldgClass,
                pluto.OwnerName,
                pluto.YearBuilt,
                pluto.YearAlter1,
                pluto.YearAlter2,
                pluto.LotArea,
                pluto.BldgArea
            FROM lots
            LEFT JOIN cases ON cases.case_number = lots.case_number AND cases.borough = lots.borough
            LEFT JOIN bids ON bids.case_number = lots.case_number
                AND bids.auction_date = cases.auction_date
                AND bids.borough = lots.borough
            LEFT JOIN pluto ON pluto.BBL = lots.BBL
            LEFT JOIN building_class ON pluto.BldgClass = building_class.id
			JOIN boroughs on lots.borough = boroughs.name;
        `);


        return { sales, lots };
    } catch (e) {
        console.error('Failed to load SQLite DB:', e);
        throw e;
    }
}


function toBase64Url(str) {
    const bytes = new TextEncoder().encode(str);
    let binary = "";
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function fromBase64Url(encoded) {
    const padded = encoded.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(encoded.length / 4) * 4, "=");
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return new TextDecoder().decode(bytes);
}

function encodeState(state) {
    return toBase64Url(JSON.stringify(state));
}

function decodeState(encoded) {
    return JSON.parse(fromBase64Url(encoded));
}

function updateURLWithFilters() {
    const state = {
        v: 2,
        filters: gridApi.getFilterModel(),
        sort: getSortState(),
    };

    const params = new URLSearchParams();
    params.set('state', encodeState(state));

    // Update the URL in the address bar
    window.history.replaceState({}, '', `${window.location.pathname}?${params}`);
}

// Create a custom control for the button
const clearTableFilter = L.Control.extend({
    options: {
        position: 'bottomright'
    },
    onAdd: function (map) {
        const container = L.DomUtil.create('button');
        container.innerHTML = 'Clear Filters';
        container.onclick = function () {
            // Update the URL in the address bar to remove all filters
            applyFiltersFromURL()
        }
        return container;
    }
});


function applyFiltersFromURL(params = null) {
    if (params === null || params.size == 0) {
        const currentDate = new Date();
        const futureDate = new Date();
        futureDate.setDate(currentDate.getDate() + 7);
        gridApi.setColumnFilterModel('auction_date', {
            dateFrom: currentDate.toISOString(),
            dateTo: futureDate.toISOString(),
            filterType: "auction_date",
            type: "inRange"
        })
        gridApi.onFilterChanged()
        return
    }
    if (params.has('state')) {
        try {
            const state = decodeState(params.get('state'));
            if (state?.filters) {
                gridApi.setFilterModel(state.filters);
            }
            if (state?.sort) {
                applySortState(state.sort);
            }
            return;
        } catch (e) {
            console.warn("Failed to parse URL state, falling back to legacy filters", e);
        }
    }

    const filters = {};
    params.forEach((value, key) => {
        filters[key] = JSON.parse(value);
    });

    gridApi.setFilterModel(filters);
}

function getSortState() {
    if (typeof gridApi.getSortModel === "function") {
        return gridApi.getSortModel();
    }
    if (typeof gridApi.getColumnState === "function") {
        return gridApi.getColumnState()
            .filter((col) => col.sort)
            .map((col) => ({ colId: col.colId, sort: col.sort, sortIndex: col.sortIndex }));
    }
    if (gridApi.columnApi && typeof gridApi.columnApi.getColumnState === "function") {
        return gridApi.columnApi.getColumnState()
            .filter((col) => col.sort)
            .map((col) => ({ colId: col.colId, sort: col.sort, sortIndex: col.sortIndex }));
    }
    return [];
}

function applySortState(sortModel) {
    if (!sortModel || sortModel.length === 0) {
        return;
    }
    if (typeof gridApi.setSortModel === "function") {
        gridApi.setSortModel(sortModel);
        return;
    }
    const state = sortModel.map((s) => ({
        colId: s.colId,
        sort: s.sort,
        sortIndex: s.sortIndex,
    }));
    if (typeof gridApi.applyColumnState === "function") {
        gridApi.applyColumnState({ state, applyOrder: false });
        return;
    }
    if (gridApi.columnApi && typeof gridApi.columnApi.applyColumnState === "function") {
        gridApi.columnApi.applyColumnState({ state, applyOrder: false });
    }
}

const propertyInfoMapUrl = (BBL, lot) => "https://propertyinformationportal.nyc.gov/parcels/" + (lot > 1000 ? "unit/" : "parcel/") + BBL

// grid columns
const columnDefs = [
    {
        headerName: "Class",
        field: "LandUse",
        valueFormatter: ({value}) => value ? toCapitalizedCase(value) : value,
        filter: 'agSetColumnFilter',
        maxWidth: 150,
    },
    {
        field: "borough",
        filter: 'agSetColumnFilter',
        maxWidth: 150,
    },
    {
        headerName: "Address",
        field: "Address",
        valueGetter: ({data}) => toCapitalizedCase(data.pluto_address ?? data.lot_address) + (data.unit ? `, Unit ${data.unit}` : ''),
        cellRenderer: 'agGroupCellRenderer',
        minWidth: 300,
    },
    {
        headerName: "Case #",
        field: "case_number",
        cellRenderer: function (params) {
            const dateStr = params.data.auction_date ? new Date(params.data.auction_date).toISOString().split('T')[0] : null;
            const base = params.value.replace('/', '-')
            const filename = dateStr
                ? `saledocs/noticeofsale/${dateStr}/${base}.pdf`
                : `saledocs/noticeofsale/${base}.pdf`;
            return `<a href="${filename}" target="_blank">` + params.value + '</a>'
        },
        minWidth: 140,
    },
    {
        headerName: "Auction Date",
        field: "auction_date",
        suppressSizeToFit: true,
        minWidth: 120,
        filter: 'agDateColumnFilter',
        sort: "asc",
        sortIndex: 0,
        filterParams: {
            minValidYear: 2024,
            maxValidYear: 2099,
            buttons: ["apply", "reset"],
            inRangeInclusive: true,
            closeOnApply: true,
            maxNumConditions: 1,
        }
    },
    {
        headerName: "BBL",
        type: "rightAligned",
        valueGetter: p => `${p.data.block}-${p.data.lot}`,
        cellRenderer: (p) => {
            const borough = String(p.data.borough_id ?? "");
            const block = String(p.data.block ?? "").padStart(5, "0");
            const lot = String(p.data.lot ?? "").padStart(4, "0");
            const bbl = borough + block + lot;
            return `<a href="${propertyInfoMapUrl(bbl, p.data.lot)}" target="_blank">` + p.value + `</a>`;
        },
        minWidth: 120,
    },
    {
        headerName: "Judgement", field: "judgement", type: ["currency", "rightAligned"],
        minWidth: 150,
    },
    {
        headerName: "Upset Price", field: "upset_price", type: ["currency", "rightAligned"],
        minWidth: 150,
    },
    {
        headerName: "Sale Price", field: "winning_bid", type: ["currency", "rightAligned"],
        cellRenderer: function (params) {
            if (params.value || params.value == "") {
                const filename = 'saledocs/surplusmoney/' + params.data.case_number.replace('/', '-') + '.pdf'
                return `<a href="${filename}" target="_blank">` + formattedCurrency.format(params.value) + '</a>'
            }
        },
        minWidth: 150,
    },
    {
        headerName: "Overbid", field: "over_bid", type: ["currency", "rightAligned"],
        minWidth: 150,
    },
    {
        headerName: "Discount", field: "price_change", type: ["percent", "rightAligned"],
        minWidth: 150,
    },
]

const formattedCurrency = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
});


const formattedPercent = new Intl.NumberFormat('en-US', {
    style: 'percent',
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
})


const defaultColDef = {
    flex: 1,
    minWidth: 100,
    filter: 'agTextColumnFilter',
    menuTabs: ['filterMenuTab'],
    autoHeaderHeight: true,
    wrapHeaderText: true,
    suppressHeaderMenuButton: true,
    sortable: true,
    resizable: true
}

function zoomToBlock(event) {
    if (!event.node.isSelected()) {
        return
    }

    const markerKey = String(event?.node?.data?.BBL);
    if (markerKey === "null" || markerKey === "undefined") {
        return;
    }
    const entry = markers[markerKey];
    if (!entry || entry.length === 0) {
        return;
    }
    map.fitBounds(entry[0].getBounds(), { maxZoom: 15 })

}

function boroughIdFromName(borough) {
    return Object.entries(borough_dict).find(([id, boro]) => boro === borough)[0]
}

const MAPPLUTO_BATCH_SIZE = 200;
const MAPPLUTO_FIELDS = [
    "BBL",
    "Address",
    "UnitsRes",
    "UnitsTotal",
    "ResArea",
    "OwnerName",
    "NumBldgs",
    "NumFloors",
    "LotArea",
    "BldgClass",
    "AssessLand",
    "AssessTot",
];

function setLoading(isLoading, message = "Loading map data…") {
    const overlay = document.getElementById("loading-overlay");
    if (!overlay) {
        return;
    }
    const text = overlay.querySelector(".loading-text");
    if (text) {
        text.textContent = message;
    }
    overlay.classList.toggle("hidden", !isLoading);
}

function setMapStatus({ total = 0, fetched = 0, missing = 0 } = {}) {
    const status = `Map: ${fetched}/${total} loaded` + (missing > 0 ? `, ${missing} missing` : "");
    map.attributionControl.setPrefix(status);
}

function debounce(fn, delayMs) {
    let t = null;
    return (...args) => {
        if (t) {
            clearTimeout(t);
        }
        t = setTimeout(() => fn(...args), delayMs);
    };
}

function chunkArray(arr, size) {
    const out = [];
    for (let i = 0; i < arr.length; i += size) {
        out.push(arr.slice(i, i + size));
    }
    return out;
}

async function fetchPlutoFeaturesByBBL(bbls) {
    if (!bbls.length) {
        return new Map();
    }

    const featuresByBbl = new Map();
    const missing = bbls.filter((bbl) => !mapFeatureCache.has(String(bbl)));
    const chunks = chunkArray(missing, MAPPLUTO_BATCH_SIZE);
    const queries = chunks.map((chunk) => new Promise((resolve) => {
        blockLotLayer.query()
            .where(`BBL IN (${chunk.join(",")})`)
            .fields(MAPPLUTO_FIELDS)
            .run((error, featureCollection) => {
                if (error) {
                    console.error("MAPPLUTO batch query failed", error);
                    resolve();
                    return;
                }

                for (const feature of featureCollection.features) {
                    const bbl = feature?.properties?.BBL;
                    if (bbl !== undefined && bbl !== null) {
                        mapFeatureCache.set(String(bbl), feature);
                    }
                }
                resolve();
            });
    }));

    await Promise.all(queries);
    for (const bbl of bbls) {
        const feature = mapFeatureCache.get(String(bbl));
        if (feature) {
            featuresByBbl.set(String(bbl), feature);
        }
    }
    return featuresByBbl;
}


async function onGridFilterChanged() {
    const seq = ++mapRenderSeq;
    markerLayer.clearLayers()
    outlineLayer.clearLayers()
    updateURLWithFilters();
    markers = {};

    const rows = [];
    const bbls = new Set();

    // Get all displayed rows
    gridApi.forEachNodeAfterFilterAndSort(({ data }) => {
        if (!data.block || !data.borough || !data.lot) {
            return
        }
        rows.push(data);
        if (data.BBL !== null && data.BBL !== undefined) {
            bbls.add(String(data.BBL));
        }
    });

    const total = bbls.size;
    setMapStatus({ total, fetched: 0, missing: total });
    setLoading(true, "Loading map data…");
    try {
        const featuresByBbl = await fetchPlutoFeaturesByBBL([...bbls]);
        if (seq !== mapRenderSeq) {
            return;
        }

        const fetched = featuresByBbl.size;
        const missingCount = Math.max(0, total - fetched);
        setMapStatus({ total, fetched, missing: missingCount });

        for (const data of rows) {
            if (seq !== mapRenderSeq) {
                return;
            }
            const onClickTableZoom = () => {
                // Highlight the row in AG Grid
                gridApi.forEachNodeAfterFilterAndSort(function (node) {
                    if (node.data.borough === data.borough && node.data.block === data.block && node.data.lot === data.lot) {
                        node.setSelected(true, true); // Select the row

                        // Ensure the selected row is visible by scrolling to it
                        gridApi.ensureIndexVisible(node.rowIndex, 'middle');
                    }
                });
            }

            if (data.BBL === null || data.BBL === undefined) {
                continue
            }

            const feature = featuresByBbl.get(String(data.BBL));
            if (!feature) {
                console.warn("failed to return any results", data.borough_code, data.BBL)
                continue;
            }

            const layer = L.geoJSON(feature, {
                onEachFeature: function (feature, layer) {
                    layer.on('click', onClickTableZoom)
                }
            }).addTo(outlineLayer);

            const centroid = getCentroid(feature.geometry)
            const p = feature.properties
            const popupContent = `
            <div>
            <h3>${p.Address}</h3>
            <ul>
                <li>UnitsRes: ${p.UnitsRes}</li>
                <li>UnitsTotal: ${p.UnitsTotal}</li>
                <li>ResArea: ${p.ResArea}</li>
                <li>OwnerName: ${p.OwnerName}</li>
                <li>NumBldgs: ${p.NumBldgs}</li>
                <li>NumFloors: ${p.NumFloors}</li>
                <li>LotArea: ${p.LotArea}</li>
                <li>BldgClass: ${p.BldgClass}</li>
                <li>AssessLand: ${p.AssessLand}</li>
                <li>AssessTot: ${p.AssessTot}</li>
                <li>LotArea: ${p.LotArea}</li>
                </ul>
            </div>
            `

            const marker = L.marker([centroid.lng, centroid.lat]).bindPopup(popupContent).addTo(markerLayer);
            marker.on('click', onClickTableZoom)

            // Store the marker in the markers object
            const markerKey = String(data.BBL);
            if (!markers[markerKey]) {
                markers[markerKey] = []
            }
            markers[markerKey].push(layer);
        }
    } finally {
        setLoading(false);
    }
}

const debouncedOnGridFilterChanged = debounce(onGridFilterChanged, 150);

// Initialize AG Grid
const gridOptions = {
    columnDefs: columnDefs,
    defaultColDef: defaultColDef,
    masterDetail: true,
    isRowMaster: (dataItem) => dataItem ? getTransactions(dataItem).length : false,
    detailRowAutoHeight: true,
    rowSelection: {
        mode: 'singleRow',
        checkboxes: false,
        enableClickSelection: true,
    },
    onRowSelected: zoomToBlock,
    // Listen for AG Grid filter changes
    onFilterChanged: debouncedOnGridFilterChanged,
    onSortChanged: updateURLWithFilters,

    columnTypes: {
        currency: {
            width: 150,
            valueFormatter: ({ value }) => value ? formattedCurrency.format(value) : value,
            filter: 'agNumberColumnFilter',
        },
        percent: {
            width: 150,
            valueFormatter: ({ value }) => value ? Number(value).toLocaleString(undefined,{style: 'percent', minimumFractionDigits:2}) : value,
            filter: 'agNumberColumnFilter',
        }
    },

    detailCellRendererParams: {
        detailGridOptions: {
            columnDefs: [
                {
                    field: 'SALE DATE',
                    headerName: 'Sale Date',
                    filter: 'agDateColumnFilter',
                    sort: "asc",
                    sortIndex: 0,
                    sortable: true
                },
                {
                    headerName: 'Sale Price',
                    field: "SALE PRICE",
                    valueFormatter: (params) => formattedCurrency.format(params.value),
                },

            ],
            defaultColDef: {
                flex: 1,
                sortable: true,
                filter: 'agNumberColumnFilter',
            },
        },
        getDetailRowData: (params) => {
            // find all transactions for this address
            let repeats = getTransactions(params.data);


            // add % price change
            repeats = repeats.map((transaction, index, arr) => {
                // For the first row, priceChange is null
                if (index === 0) {
                    return { ...transaction, priceChange: null };
                }

                // For subsequent rows, priceChange is the difference in price over the immediately preceding sale
                const priceChange = transaction["SALE PRICE"] - arr[index - 1]["SALE PRICE"];
                const priceChangePct = transaction["SALE PRICE"] / arr[index - 1]["SALE PRICE"] - 1;
                return { ...transaction, priceChange, priceChangePct };
            });

            params.successCallback(repeats);
        },
    },

};

// Create AG Grid
const gridDiv = document.querySelector('#myGrid');
const gridApi = agGrid.createGrid(gridDiv, gridOptions)


// Load from DB only
loadDB().then(({ sales, lots }) => {
    combinedData = sales

    lots.filter(({winning_bid}) => winning_bid > 100).forEach(lot => {
        const transactions = getTransactions(lot)

        if (transactions.length > 0) {
            const last_sale = transactions[transactions.length - 1]
            lot.price_change = lot.winning_bid / last_sale["SALE PRICE"] - 1
        } 

    });

    // load the full table
    gridApi.setGridOption('rowData', lots)
    gridApi.sizeColumnsToFit()
    // Load and apply filters from URL when the grid initializes (have to wait till now so that table isn't empty)
    applyFiltersFromURL(new URLSearchParams(window.location.search));
})
    .catch(error => {
        console.error('Error loading SQLite database:', error);
    });


function getTransactions(data) {
    let repeats = combinedData.filter(({ BOROUGH, BLOCK, LOT, "SALE PRICE": SALE_PRICE }) => BOROUGH == data.borough && BLOCK == data.block && LOT == data.lot && SALE_PRICE > minTransactionPrice);
    repeats.sort((a, b) => a["SALE DATE"] - b["SALE DATE"]);
    return repeats;
}

// Style URL format in XYZ PNG format; see our documentation for more options
const toner = L.tileLayer('https://tiles.stadiamaps.com/tiles/stamen_toner/{z}/{x}/{y}{r}.png', {
    maxZoom: 20,
    attribution: '&copy; <a href="https://stadiamaps.com/" target="_blank">Stadia Maps</a>, &copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap</a>',
});


const map = L.map('map', {
    center: [40.7143, -74.0060],
    zoom: 13,
    layers: [toner]
})
const layerControl = L.control.layers({ "Streets": toner }).addTo(map);

const blockLotLayer = L.esri.featureLayer({
    url: 'https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/MAPPLUTO/FeatureServer/0',
    where: "1 = 0"
}).addTo(map);

const markerLayer = L.markerClusterGroup();
map.addLayer(markerLayer)
const outlineLayer = L.layerGroup().addTo(map);

map.on("zoomend", function() {
    if (map.getZoom() < 15) {
        if (map.hasLayer(outlineLayer)) {
            map.removeLayer(outlineLayer);
        }
    }
    else {
        if (!map.hasLayer(outlineLayer)) {
            map.addLayer(outlineLayer);
        }
    }
});


// Add the custom control to the map
map.addControl(new clearTableFilter());

// Function to calculate the centroid of a GeoJSON geometry
function getCentroid(geometry) {
    let latlng = [];

    switch (geometry.type) {
        case 'Polygon':
            latlng = L.polygon(geometry.coordinates).getBounds().getCenter();
            break;
        case 'MultiPolygon':
            latlng = L.polygon(geometry.coordinates[0]).getBounds().getCenter();
            break;
        case 'Point':
            latlng = L.latLng(geometry.coordinates[1], geometry.coordinates[0]);
            break;
        default:
            console.error('Unsupported geometry type:', geometry.type);
            break;
    }
    return latlng;
}



// splitter functionality
const splitter = document.getElementById('splitter')

let isResizing = false
const mapDiv = document.getElementById('map')

splitter.addEventListener('mousedown', startResize())
document.addEventListener('mousemove', resize())
document.addEventListener('mouseup', stopResize())

splitter.addEventListener('touchstart', startResize())
document.addEventListener('touchmove', resize())
document.addEventListener('touchend', stopResize())

function stopResize() {
    return () => {
        isResizing = false;
    };
}

function resize() {
    return (e) => {
        if (!isResizing) return;
        const lastY = e.type.includes('mouse') ? e.clientY : e.touches[0].clientY;
        const newMapHeight = lastY;
        const newGridHeight = window.innerHeight - lastY - splitter.offsetHeight;

        mapDiv.style.height = newMapHeight + 'px';
        gridDiv.style.height = newGridHeight + 'px';
        map.invalidateSize();
        gridApi.sizeColumnsToFit();

    };
}

function startResize() {
    return (e) => {
        isResizing = true;
    };
}

function toCapitalizedCase(str) {
    if (!str) return str;
    return str
      .toLowerCase() // Convert the entire string to lowercase first
      .split(' ') // Split the string into words based on spaces
      .map(word => word.charAt(0).toUpperCase() + word.slice(1)) // Capitalize the first letter of each word
      .join(' '); // Join the words back into a single string
  }
