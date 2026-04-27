import { UploadCloud, ShieldAlert } from "lucide-react"
import { useAuth } from "@/auth/AuthContext"
import { useBreakpoint } from "@/context/BreakpointContext"
import { ShadcnFileUploader } from "@/components/ShadcnFileUploader"
import { ShadcnRegistryList } from "@/components/ShadcnRegistryList"
import { Section } from "@radix-ui/themes"
import { Separator } from "@/components/ui/separator"
import { Card, CardContent } from "@/components/ui/card"

export function ShadcnImportSection() {
  const { isAuthenticated } = useAuth()
  const screenSize = useBreakpoint()

  if (!isAuthenticated) {
    return (
      <Section size="2">
        <Card className="border-destructive/50 bg-destructive/5">
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-destructive mb-2">
              <ShieldAlert className="h-5 w-5" />
              <p className="font-semibold">Authentication Required</p>
            </div>
            <p className="text-sm text-muted-foreground">
              You must be logged in to access the Import tools and Registry.
            </p>
          </CardContent>
        </Card>
      </Section>
    )
  }

  const isStacked = ["xsm", "sm"].includes(screenSize)
  const layoutDirection = isStacked ? "flex-col" : "flex-row"

  return (
    <Section size="2" className="w-full">
      <div className="flex flex-col gap-6 w-full">
        {/* Section Header */}
        <div className="flex items-center gap-4">
          <div className="bg-primary p-2.5 rounded-lg shadow-sm text-primary-foreground">
            <UploadCloud className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold tracking-tight">Data Ingestion Hub</h2>
            <p className="text-sm text-muted-foreground">Import local files into the federated registry</p>
          </div>
        </div>

        <Separator />

        {/* FLEX-STANDARD: Row on Desktop, Column on Mobile */}
        <div className={`flex ${layoutDirection} gap-6 items-start w-full`}>
          {/* Col 1: Uploader */}
          <ShadcnFileUploader />

          {/* Col 2: Registry */}
          <ShadcnRegistryList />
        </div>
      </div>
    </Section>
  )
}
