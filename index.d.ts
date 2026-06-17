import type {Duplex} from 'stream';
import type {Feature, FeatureCollection} from 'geojson';

export interface ConvertOptions {
  layer?: string;
  fieldLengths?: Record<string, number>;
  padValues?: boolean;
  schema?: Array<{ name: string; type: string; length?: number; precision?: number }>;
  targetCrs?: number;
  sourceCrs?: number;
  tmpDir?: string;
  returnAsFileStreams?: boolean;
}

export function convert(
  input: FeatureCollection | Feature[] | NodeJS.ReadableStream | string,
  output: NodeJS.WritableStream | string,
  options?: ConvertOptions
): Promise<void>;

export function createConvertStream(options?: ConvertOptions): Duplex;
