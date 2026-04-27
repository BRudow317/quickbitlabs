import type { CSSProperties, HTMLAttributes } from 'react'
import { Box, Flex, Text } from '@radix-ui/themes'
import { cn } from '@/lib/utils'
import '@/styles/visual-globe.css'

type GlobeSpeed = 'slow' | 'normal' | 'fast'

const SPEED_SECONDS: Record<GlobeSpeed, number> = {
  slow: 24,
  normal: 16,
  fast: 10,
}

export interface VisualGlobeProps extends HTMLAttributes<HTMLDivElement> {
  size?: number | string
  backgroundColor?: string
  globeColor?: string
  gridColor?: string
  orbitColor?: string
  ringColor?: string
  showGlow?: boolean
  glowColor?: string
  spin?: boolean
  pulse?: boolean
  shimmer?: boolean
  speed?: GlobeSpeed
  statusText?: string
  showStatus?: boolean
}

export function VisualGlobe({
  className,
  size = 220,
  backgroundColor = 'transparent',
  globeColor = '#2a2820',
  gridColor = '#f4f3ef',
  orbitColor = '#d9d6cb',
  ringColor = '#2a2820',
  showGlow = true,
  glowColor = 'rgba(42, 40, 32, 0.18)',
  spin = true,
  pulse = true,
  shimmer = true,
  speed = 'normal',
  statusText = 'Unpacking...',
  showStatus = false,
  ...props
}: VisualGlobeProps) {
  const styleVars = {
    '--qbl-globe-size': typeof size === 'number' ? `${size}px` : size,
    '--qbl-globe-bg': backgroundColor,
    '--qbl-globe-color': globeColor,
    '--qbl-globe-grid': gridColor,
    '--qbl-globe-orbit': orbitColor,
    '--qbl-globe-ring': ringColor,
    '--qbl-glow-color': glowColor,
    '--qbl-rotate-duration': `${SPEED_SECONDS[speed]}s`,
  } as CSSProperties

  return (
    <Flex
      direction="column"
      align="center"
      gap="2"
      className={cn(
        'qbl-globe-root',
        spin && 'qbl-globe-spin',
        pulse && 'qbl-globe-pulse',
        shimmer && 'qbl-globe-shimmer',
        className,
      )}
      style={styleVars}
      {...props}
    >
      <Box className="qbl-globe-stage" aria-hidden="true">
        {showGlow ? <Box className="qbl-globe-glow" /> : null}
        <svg
          className="qbl-globe-svg"
          viewBox="0 0 200 200"
          role="img"
          aria-label="Decorative globe"
          xmlns="http://www.w3.org/2000/svg"
        >
          <circle className="qbl-globe-core" cx="100" cy="100" r="62" />
          <circle className="qbl-globe-ring" cx="100" cy="100" r="62" fill="none" strokeWidth="1.5" />
          <g className="qbl-globe-rotor">
            <ellipse cx="100" cy="100" rx="30" ry="62" fill="none" className="qbl-globe-grid" strokeWidth="1" />
            <line x1="38" y1="100" x2="162" y2="100" className="qbl-globe-grid" strokeWidth="0.8" />
            <line x1="48" y1="68" x2="152" y2="68" className="qbl-globe-grid" strokeWidth="0.6" />
            <line x1="48" y1="132" x2="152" y2="132" className="qbl-globe-grid" strokeWidth="0.6" />
          </g>
          <g className="qbl-globe-orbits">
            <path d="M 100 34 A 16 16 0 0 1 105 97" className="qbl-globe-orbit orbit-1" fill="none" strokeWidth="2.5" strokeLinecap="round" />
            <path d="M 166 100 A 16 16 0 0 1 103 106" className="qbl-globe-orbit orbit-2" fill="none" strokeWidth="2" strokeLinecap="round" />
            <path d="M 100 166 A 18 18 0 0 1 94 102" className="qbl-globe-orbit orbit-3" fill="none" strokeWidth="1.5" strokeLinecap="round" />
          </g>
        </svg>
      </Box>

      {showStatus ? (
        <Text size="1" className="qbl-globe-status">
          {statusText}
        </Text>
      ) : null}
    </Flex>
  )
}

export default VisualGlobe