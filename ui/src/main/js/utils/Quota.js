const UNITS = ['MiB', 'GiB', 'TiB', 'PiB', 'EiB'];

export function formatMb(sizeInMb) {
  const unitIdx = (sizeInMb > 0) ? Math.floor(Math.log(sizeInMb) / Math.log(1024)) : 0;
  return (sizeInMb / Math.pow(1024, unitIdx)).toFixed(2) + '' + UNITS[unitIdx];
}
