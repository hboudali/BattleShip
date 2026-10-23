/**
 * Battleship Game Logic
 *
 * Pure, DOM-free helpers for board setup and shot resolution.
 * Classic rules: 10x10 grid, fleet of 5, 4, 3, 3, 2.
 */

export const SIZE = 10;

export const FLEET = [
  { id: 'carrier', name: 'Carrier', length: 5 },
  { id: 'battleship', name: 'Battleship', length: 4 },
  { id: 'cruiser', name: 'Cruiser', length: 3 },
  { id: 'submarine', name: 'Submarine', length: 3 },
  { id: 'destroyer', name: 'Destroyer', length: 2 }
];

// No 0/O or 1/I/L, so codes are easy to read aloud
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

/**
 * Generate a short game code
 * @param {number} length - Number of characters
 * @returns {string} Code such as "K7Q2M"
 */
export function generateCode(length = 5) {
  const values = crypto.getRandomValues(new Uint32Array(length));
  return Array.from(values, v => CODE_ALPHABET[v % CODE_ALPHABET.length]).join('');
}

/**
 * Normalize user-entered code (uppercase, strip spaces/dashes)
 * @param {string} raw - Raw input
 * @returns {string} Normalized code
 */
export function normalizeCode(raw) {
  return (raw || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

/**
 * Cells covered by a ship
 * @param {Object} ship - { row, col, length, vertical }
 * @returns {Array<[number, number]>} Cell coordinates
 */
export function shipCells({ row, col, length, vertical }) {
  return Array.from({ length }, (_, i) => (vertical ? [row + i, col] : [row, col + i]));
}

/**
 * Check whether a ship can be placed
 * @param {Array<Object>} ships - Ships already placed
 * @param {Object} candidate - Ship to place
 * @returns {boolean} True when inside the grid and not overlapping
 */
export function canPlace(ships, candidate) {
  const cells = shipCells(candidate);
  if (cells.some(([r, c]) => r < 0 || c < 0 || r >= SIZE || c >= SIZE)) return false;
  const taken = new Set(
    ships.filter(s => s.id !== candidate.id).flatMap(s => shipCells(s).map(([r, c]) => `${r},${c}`))
  );
  return cells.every(([r, c]) => !taken.has(`${r},${c}`));
}

/**
 * Place the whole fleet randomly
 * @returns {Array<Object>} Placed ships
 */
export function randomFleet() {
  const ships = [];
  for (const { id, length } of FLEET) {
    let ship;
    do {
      const vertical = Math.random() < 0.5;
      ship = {
        id,
        length,
        vertical,
        row: Math.floor(Math.random() * (vertical ? SIZE - length + 1 : SIZE)),
        col: Math.floor(Math.random() * (vertical ? SIZE : SIZE - length + 1))
      };
    } while (!canPlace(ships, ship));
    ships.push(ship);
  }
  return ships;
}

/**
 * Find the ship occupying a cell
 * @param {Array<Object>} ships - Placed ships
 * @param {number} row - Row index
 * @param {number} col - Column index
 * @returns {Object|undefined} Ship or undefined
 */
export function shipAt(ships, row, col) {
  return ships.find(s => shipCells(s).some(([r, c]) => r === row && c === col));
}

/**
 * Resolve an incoming shot against the defender's fleet
 * @param {Array<Object>} ships - Defender's ships
 * @param {Array<Object>} received - Shots already received ({ row, col })
 * @param {number} row - Target row
 * @param {number} col - Target column
 * @returns {Object} { row, col, hit, sunk: ship|null, gameOver }
 */
export function resolveShot(ships, received, row, col) {
  const ship = shipAt(ships, row, col);
  const hits = new Set([...received, { row, col }].map(s => `${s.row},${s.col}`));
  const isSunk = s => shipCells(s).every(([r, c]) => hits.has(`${r},${c}`));
  const sunk = ship && isSunk(ship) ? ship : null;
  return { row, col, hit: Boolean(ship), sunk, gameOver: ships.every(isSunk) };
}
