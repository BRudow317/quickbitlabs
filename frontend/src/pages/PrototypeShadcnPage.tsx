import { Container, Heading, Flex, Text, Box } from '@radix-ui/themes';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';
import { FlaskConical, Rocket } from 'lucide-react';

export function PrototypeShadcnPage() {
  return (
    <Box>
      <Box style={{ background: 'var(--gray-2)', borderBottom: '1px solid var(--gray-4)' }}>
        <Container size="4">
          <Flex direction="column" gap="2" py="6">
            <Heading size="8" weight="bold">Shadcn Lab</Heading>
            <Text color="gray" size="3">
              Testing ground for Shadcn UI components integrated with Tailwind v4 and Radix.
            </Text>
          </Flex>
        </Container>
      </Box>

      <Container size="4">
        <Flex direction="column" gap="6" py="6">
          
          <Section title="Buttons">
            <Flex gap="3" wrap="wrap">
              <Button>Default</Button>
              <Button variant="secondary">Secondary</Button>
              <Button variant="destructive">Destructive</Button>
              <Button variant="outline">Outline</Button>
              <Button variant="ghost">Ghost</Button>
              <Button variant="link">Link</Button>
              <Button size="icon"><Rocket size={16} /></Button>
            </Flex>
          </Section>

          <Section title="Cards">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card>
                <CardHeader>
                  <CardTitle>Shadcn Card</CardTitle>
                  <CardDescription>Built with Tailwind v4</CardDescription>
                </CardHeader>
                <CardContent>
                  <Text size="2">
                    This card is a native Shadcn component. It should follow the project's
                    color tokens via the Tailwind-Radix bridge.
                  </Text>
                </CardContent>
                <CardFooter>
                  <Button className="w-full">Action</Button>
                </CardFooter>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <FlaskConical size={18} /> Lab Notes
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="list-disc pl-5 space-y-2 text-sm">
                    <li>Tailwind v4 is active</li>
                    <li>Radix Themes provides the layout containers</li>
                    <li>Shadcn provides the functional primitives</li>
                  </ul>
                </CardContent>
              </Card>
            </div>
          </Section>

        </Flex>
      </Container>
    </Box>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <Flex direction="column" gap="3">
      <Heading size="4" weight="bold" color="gray" style={{ textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        {title}
      </Heading>
      <Box p="4" style={{ background: 'var(--color-background)', border: '1px solid var(--gray-4)', borderRadius: 'var(--radius-4)' }}>
        {children}
      </Box>
    </Flex>
  );
}
