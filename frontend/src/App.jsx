// import { useState } from 'react'
import Map, { Source, Layer, Popup } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useEffect, useState } from 'react';
import axios from 'axios';

function App() {

    const [stats,setStats] = useState(null)
    const [assets,setAssets] = useState(null)
    const [selectedAsset, setSelectedAsset] = useState(null)

    useEffect(() => {
        async function fetchData() {
            const data = (await axios.get("https://api.poo.deveroonie.co.uk/api/stats")).data
            const assetData = (await axios.get("https://api.poo.deveroonie.co.uk/api/assets")).data.assets
            setStats(data)
            setAssets(assetData)
        }
        fetchData()
    }, [])
    
    if(!stats || !assets) return <div>Loading...</div>

    const geojson = {
        type: 'FeatureCollection',
        features: assets.map(asset => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [asset.longitude, asset.latitude] },
            properties: { status: asset.status, name: asset.name, asset_id: asset.asset_id }
        }))
    }

    const circleLayer = {
        id: 'assets',
        type: 'circle',
        paint: {
            'circle-radius': 4,
            'circle-color': ['match', ['get', 'status'], 0, 'green', 1, 'red', 'gray'],
            'circle-opacity': 0.85
            //'circle-sort-key': ['match', ['get', 'status'], 0, 0, 1, 2, 1],
        }
    }

    return (
      <>
      <div className='flex flex-col h-full'>
          <div className='shrink-0 bg-brown text-white p-8 text-center'>
            <span className='text-3xl font-bold'>Poo</span>
            <p className='text-lg'>A live map showing sewage discharges from all water companies across England, Wales* and Scotland.</p>
            <span className='text-gray-300 italic'>*excludes Hafren Dyfrdwy</span><br />
            <p className='text-lg font-semibold'>
              <span className='p-2 rounded-lg font-bold bg-brown-800'>{stats.total_discharging}</span> CSOs discharging right now.
            </p>
          </div>
          <div className="grow">
            <Map
            initialViewState={{ longitude: -0.1276, latitude: 51.5074, zoom: 10 }}
            style={{ width: '100%', height: '100%' }}
            mapStyle="https://tiles.openfreemap.org/styles/liberty"
            onClick={e => {
              const feature = e.features?.[0]
              if (feature) {
                setSelectedAsset({
                  longitude: e.lngLat.lng,
                  latitude: e.lngLat.lat,
                  ...feature.properties
                })
              } else {
                setSelectedAsset(null)
              }
            }}
            interactiveLayerIds={['assets']}
            cursor={selectedAsset ? 'pointer' : 'auto'}
          >
            <Source id="assets" type="geojson" data={geojson}>
              <Layer {...circleLayer} />
            </Source>
            {selectedAsset && (
              <Popup
                longitude={selectedAsset.longitude}
                latitude={selectedAsset.latitude}
                anchor="bottom"
                onClose={() => setSelectedAsset(null)}
                closeOnClick={false}
              >
                <AssetPopup assetId={selectedAsset.asset_id} status={selectedAsset.status} />
              </Popup>
            )}
            
          </Map>
          </div>
      </div>
      <h2 className='text-center text-2xl font-semibold mt-6 mb-4'>Overflows by Company</h2>
      <div className='flex flex-col items-center px-4 pb-8'>
        <table className='w-full max-w-4xl text-sm border-collapse'>
          <thead>
            <tr className='bg-gray-100 text-gray-700 uppercase text-xs tracking-wide'>
              <th className='text-left px-4 py-2 border border-gray-200'>Company</th>
              <th className='text-right px-4 py-2 border border-gray-200'>Total CSOs</th>
              <th className='text-right px-4 py-2 border border-gray-200'>Active</th>
              <th className='text-right px-4 py-2 border border-gray-200'>Active %</th>
              <th className='text-right px-4 py-2 border border-gray-200'>Offline</th>
            </tr>
          </thead>
          <tbody>
            {stats.companies.map((company, i) => (
              <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                <td className='px-4 py-2 border border-gray-200 font-medium'>{company.company}</td>
                <td className='px-4 py-2 border border-gray-200 text-right tabular-nums'>{company.total_assets}</td>
                <td className='px-4 py-2 border border-gray-200 text-right tabular-nums'>
                  <span style={{ color: company.total_discharging > 0 ? 'red' : 'inherit', fontWeight: company.total_discharging > 0 ? '600' : 'normal' }}>
                    {company.total_discharging}
                  </span>
                </td>
                <td className='px-4 py-2 border border-gray-200 text-right tabular-nums'>{company.percent_active}%</td>
                <td className='px-4 py-2 border border-gray-200 text-right tabular-nums text-gray-500'>{company.company == "Dwr Cymru Welsh Water" ? "Data Unreliable." : company.total_offline}</td>
              </tr>
            ))}
            {(() => {
              const totalAssets = stats.companies.reduce((s, c) => s + c.total_assets, 0)
              const totalDischarging = stats.companies.reduce((s, c) => s + c.total_discharging, 0)
              const totalOffline = stats.companies.reduce((s, c) => s + c.total_offline, 0)
              const pct = totalAssets > 0 ? ((totalDischarging / totalAssets) * 100).toFixed(1) : '0.0'
                return (
                <tr className='bg-gray-100 font-semibold border-t-2 border-gray-400'>
                  <td className='px-4 py-2 border border-gray-200'>Total</td>
                  <td className='px-4 py-2 border border-gray-200 text-right tabular-nums'>{totalAssets}</td>
                  <td className='px-4 py-2 border border-gray-200 text-right tabular-nums'>
                  <span style={{ color: totalDischarging > 0 ? 'red' : 'inherit', fontWeight: totalDischarging > 0 ? '600' : 'normal' }}>
                    {totalDischarging}
                  </span>
                  </td>
                  <td className='px-4 py-2 border border-gray-200 text-right tabular-nums'>{pct}%</td>
                  <td className='px-4 py-2 border border-gray-200 text-right tabular-nums text-gray-500'>{totalOffline - (stats.companies.find(c => c.company === "Dwr Cymru Welsh Water")?.total_offline || 0)}</td>
                </tr>
                )
            })()}
          </tbody>
        </table>
      </div>
            <div className='shrink-0 bg-brown text-white p-8 text-center'>
            <span className='text-3xl font-bold'>Poo</span>
            <p className='text-xl font-semibold'>&copy; 2026 <a href='https://deveroonie.co.uk'>Deveroonie</a>. All rights reserved</p>
            <span className='text-lg'>Uses data from<br /></span>
              <span className='text-sm'>
                <a href='https://www.streamwaterdata.co.uk/'>Stream Water Data</a>,
                Anglian Water Services Limited,
                Dŵr Cymru Cyf,
                Northumbrian Water Ltd,&nbsp;
                <a href='https://www.scottishwater.co.uk/help-and-resources/open-data/overflow-map-data'>Scottish Water</a>,
                Severn Trent Water Limited,
                South West Water Limited,
                Southern Water Services Limited,
                Thames Water Utilities Limited,
                United Utilities Group plc,
                Wessex Water Services Limited,
                Yorkshire Water Services Limited
              </span>
            <span className='text-lg'><br />Map Data provided by<br /></span>
            <span className='text-sm'>
              <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a>,&nbsp;
              <a href='https://openmaptiles.org/'>OpenMapTiles</a>,&nbsp;
              <a href='https://maplibre.org/'>MapLibre</a>,&nbsp;
              <a href='https://openfreemap.org/'>OpenFreeMap</a>
            </span>
          </div>
      </>
    )
}

function AssetPopup({ assetId, status }) {
  const [data, setData] = useState(null)
  const [events, setEvents] = useState(null)
  const [tab, setTab] = useState('info')

  useEffect(() => {
    setData(null)
    setEvents(null)
    setTab('info')
    axios.get(`https://api.poo.deveroonie.co.uk/api/asset/${assetId}`)
      .then(res => setData(res.data))
    axios.get(`https://api.poo.deveroonie.co.uk/api/asset/${assetId}/events`)
      .then(res => {
        const ev = res.data.events ?? res.data
        setEvents(Array.isArray(ev) ? ev : [])
      })
      .catch(() => setEvents([]))
  }, [assetId])

  if (!data) return <div style={{ minWidth: '220px' }}>Loading...</div>

  const statusColor = status === 0 ? 'green' : status === 1 ? 'red' : 'gray'
  const statusLabel = status === 0 ? 'Not discharging' : status === 1 ? 'Discharging' : 'Monitor Offline'

  const hoursActive = data.latest_event_start
    // eslint-disable-next-line react-hooks/purity
    ? ((Date.now() - new Date(data.latest_event_start).getTime()) / 3600000).toFixed(1)
    : null

  const tabStyle = (t) => ({
    flex: 1,
    padding: '3px 0',
    fontSize: '12px',
    fontWeight: tab === t ? '600' : 'normal',
    background: 'none',
    border: 'none',
    borderBottom: tab === t ? '2px solid #374151' : '2px solid transparent',
    cursor: 'pointer',
    color: tab === t ? '#111827' : '#6b7280',
    textTransform: 'capitalize',
  })

  return (
    <div style={{ minWidth: '220px' }}>
      <p className='font-semibold text-sm'>{data.company}</p>
      <p className='font-semibold text-xs text-gray-700'>{data.asset_id}</p>
      {data.receiving_watercourse && <p className='text-xs text-gray-500'>{data.receiving_watercourse}</p>}
      <div style={{ display: 'flex', borderBottom: '1px solid #e5e7eb', margin: '6px 0' }}>
        <button style={tabStyle('info')} onClick={() => setTab('info')}>Info</button>
        <button style={tabStyle('history')} onClick={() => setTab('history')}>History</button>
      </div>

      {tab === 'info' && (
        <div>
          <p className='text-sm'>
            Status: <span style={{ color: statusColor, fontWeight: 'bold' }}>{statusLabel}</span>
          </p>
          {data.latest_event_start && (
            <p className='text-xs text-gray-500 mt-1'>
              Since: {new Date(data.latest_event_start).toLocaleString()}
            </p>
          )}
          {hoursActive !== null && status === 1 && (
            <p className='text-xs text-gray-500'>
              Active for: <span style={{ fontWeight: '600' }}>{hoursActive}h</span>
            </p>
          )}
        </div>
      )}

      {tab === 'history' && (
        <div style={{ maxHeight: '160px', overflowY: 'auto' }}>
          {events === null ? (
            <p className='text-xs text-gray-500'>Loading...</p>
          ) : events.length === 0 ? (
            <p className='text-xs text-gray-500'>No events found.</p>
          ) : (
            events.map((ev, i) => {
              const start = new Date(ev.event_start)
              const end = ev.event_end ? new Date(ev.event_end) : null
              const duration = end
                ? ((end - start) / 3600000).toFixed(1)
                // eslint-disable-next-line react-hooks/purity
                : ((Date.now() - start.getTime()) / 3600000).toFixed(1)
              return (
                <div key={i} style={{ borderBottom: '1px solid #f3f4f6', paddingBottom: '4px', marginBottom: '4px' }}>
                  <p className='text-xs font-medium'>{start.toLocaleString()}</p>
                  <p className='text-xs text-gray-500'>
                    {end ? `Ended: ${end.toLocaleString()}` : 'Ongoing'} &middot; {duration}h
                  </p>
                </div>
              )
            })
          )}
        </div>
      )}
    </div>
  )
}

export default App
